#!/usr/bin/env bash
#
# redrum.sh — deploy, monitor, and kill the Pinecone load generators
#
# Usage:
#   ./redrum.sh deploy                   Build images, push to ECR, start ECS services
#   ./redrum.sh status                   Show running task status
#   ./redrum.sh logs [writer|querier]    Tail live logs
#   ./redrum.sh kill                     Stop both services
#   ./redrum.sh destroy                  Stop services AND delete all AWS resources
#   ./redrum.sh autoscale enable         Register CPU-based auto-scaling policies
#   ./redrum.sh autoscale disable        Remove auto-scaling policies
#   ./redrum.sh ramp <writers> <queriers>  Gradually step to target counts over 5 min
#
set -euo pipefail

# load .env if present
if [[ -f "$(dirname "${BASH_SOURCE[0]}")/.env" ]]; then
  set -o allexport
  source "$(dirname "${BASH_SOURCE[0]}")/.env"
  set +o allexport
fi

# ---------------------------------------------------------------------------
# Configuration — override any of these with environment variables
# ---------------------------------------------------------------------------
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-}"
CLUSTER="${CLUSTER:-redrum}"
LOG_GROUP="${LOG_GROUP:-/redrum}"

INDEX_HOST="${INDEX_HOST:-}"
PINECONE_API_KEY="${PINECONE_API_KEY:-}"   # required — set this before running deploy

VECTOR_DIM="${VECTOR_DIM:-1024}"
WRITE_COUNT="${WRITE_COUNT:-200}"
QUERY_COUNT="${QUERY_COUNT:-10}"
TOP_K="${TOP_K:-10}"
MIN_SLEEP_SECONDS="${MIN_SLEEP_SECONDS:-60}"
MAX_SLEEP_SECONDS="${MAX_SLEEP_SECONDS:-600}"
WRITER_COUNT="${WRITER_COUNT:-1}"
QUERIER_COUNT="${QUERIER_COUNT:-1}"

DYNAMO_TABLE="${DYNAMO_TABLE:-redrum-freshness}"
METRICS_TABLE="${METRICS_TABLE:-redrum-metrics}"
RECALL_TABLE="${RECALL_TABLE:-redrum-recall}"
STATS_TABLE="${STATS_TABLE:-redrum-index-stats}"
SSM_FLAG_PATH="${SSM_FLAG_PATH:-/redrum/freshness_enabled}"
LAMBDA_FUNCTION="${LAMBDA_FUNCTION:-redrum-tracker}"
TRACKER_TIMEOUT="${TRACKER_TIMEOUT:-200}"   # seconds — probe runs 180s, needs buffer to return
RECALL_TIMEOUT="${RECALL_TIMEOUT:-60}"
STATS_TIMEOUT="${STATS_TIMEOUT:-30}"

# Autoscaling config
WRITER_MIN="${WRITER_MIN:-1}"
WRITER_MAX="${WRITER_MAX:-10}"
QUERIER_MIN="${QUERIER_MIN:-1}"
QUERIER_MAX="${QUERIER_MAX:-20}"
SCALE_OUT_CPU="${SCALE_OUT_CPU:-60}"
SCALE_IN_CPU="${SCALE_IN_CPU:-20}"
RAMP_STEPS="${RAMP_STEPS:-5}"
RAMP_STEP_SECONDS="${RAMP_STEP_SECONDS:-60}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
AWS="aws --region $AWS_REGION${AWS_PROFILE:+ --profile $AWS_PROFILE}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; RESET='\033[0m'

info()  { echo -e "${BLUE}▶ $*${RESET}"; }
ok()    { echo -e "${GREEN}✓ $*${RESET}"; }
err()   { echo -e "${RED}✗ $*${RESET}" >&2; exit 1; }

account_id() { $AWS sts get-caller-identity --query Account --output text; }
ecr_base()   { echo "$(account_id).dkr.ecr.$AWS_REGION.amazonaws.com"; }

# ---------------------------------------------------------------------------
# deploy
# ---------------------------------------------------------------------------
cmd_deploy() {
  [[ -z "$PINECONE_API_KEY" ]] && err "PINECONE_API_KEY is not set. Export it before running deploy."

  ACCOUNT_ID=$(account_id)
  ECR_BASE="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

  # --- DynamoDB tables ---
  info "Creating DynamoDB tables..."

  $AWS dynamodb describe-table --table-name "$DYNAMO_TABLE" &>/dev/null || \
    $AWS dynamodb create-table \
      --table-name "$DYNAMO_TABLE" \
      --attribute-definitions AttributeName=id,AttributeType=S \
      --key-schema AttributeName=id,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE \
      > /dev/null
  ok "DynamoDB table: $DYNAMO_TABLE"

  $AWS dynamodb describe-table --table-name "$METRICS_TABLE" &>/dev/null || \
    $AWS dynamodb create-table \
      --table-name "$METRICS_TABLE" \
      --attribute-definitions \
        AttributeName=metric_type,AttributeType=S \
        AttributeName=ts,AttributeType=N \
      --key-schema \
        AttributeName=metric_type,KeyType=HASH \
        AttributeName=ts,KeyType=RANGE \
      --billing-mode PAY_PER_REQUEST \
      > /dev/null
  ok "DynamoDB table: $METRICS_TABLE"

  $AWS dynamodb describe-table --table-name "$RECALL_TABLE" &>/dev/null || \
    $AWS dynamodb create-table \
      --table-name "$RECALL_TABLE" \
      --attribute-definitions AttributeName=id,AttributeType=S \
      --key-schema AttributeName=id,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      > /dev/null
  ok "DynamoDB table: $RECALL_TABLE"

  $AWS dynamodb describe-table --table-name "$STATS_TABLE" &>/dev/null || \
    $AWS dynamodb create-table \
      --table-name "$STATS_TABLE" \
      --attribute-definitions \
        AttributeName=date,AttributeType=S \
        AttributeName=ts,AttributeType=N \
      --key-schema \
        AttributeName=date,KeyType=HASH \
        AttributeName=ts,KeyType=RANGE \
      --billing-mode PAY_PER_REQUEST \
      > /dev/null
  ok "DynamoDB table: $STATS_TABLE"

  # --- SSM parameter (default off) ---
  info "Initialising SSM flag $SSM_FLAG_PATH..."
  $AWS ssm put-parameter --name "$SSM_FLAG_PATH" --value "false" \
    --type String --overwrite > /dev/null
  ok "SSM flag: $SSM_FLAG_PATH = false"

  # --- ECR repos ---
  info "Creating ECR repositories..."
  for repo in redrum-writer redrum-querier redrum-tracker redrum-recall redrum-index-stats; do
    $AWS ecr describe-repositories --repository-names "$repo" &>/dev/null || \
      $AWS ecr create-repository --repository-name "$repo" --image-scanning-configuration scanOnPush=true > /dev/null
    ok "$repo"
  done

  # --- Docker login ---
  info "Logging into ECR..."
  $AWS ecr get-login-password | docker login --username AWS --password-stdin "$ECR_BASE" > /dev/null
  ok "Docker authenticated"

  # --- Build & push ---
  for svc in writer querier tracker recall index-stats; do
    info "Building redrum-$svc..."
    docker build -t "redrum-$svc" "$SCRIPT_DIR/$svc" --platform linux/amd64 -q
    docker tag "redrum-$svc:latest" "$ECR_BASE/redrum-$svc:latest"
    info "Pushing redrum-$svc..."
    docker push "$ECR_BASE/redrum-$svc:latest" > /dev/null
    ok "redrum-$svc pushed"
  done

  # --- CloudWatch log group ---
  info "Creating CloudWatch log group $LOG_GROUP..."
  $AWS logs create-log-group --log-group-name "$LOG_GROUP" 2>/dev/null || true
  ok "Log group ready"

  # --- ECS task execution role ---
  info "Setting up ECS task execution role..."
  ROLE_NAME="redrumTaskExecutionRole"
  TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  $AWS iam get-role --role-name "$ROLE_NAME" &>/dev/null || \
    $AWS iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "$TRUST" > /dev/null
  $AWS iam attach-role-policy --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy" 2>/dev/null || true
  EXECUTION_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME"
  ok "Execution role: $EXECUTION_ROLE_ARN"

  # --- ECS task role (writer needs DynamoDB + SSM) ---
  info "Setting up ECS task role for writer..."
  TASK_ROLE_NAME="redrumTaskRole"
  $AWS iam get-role --role-name "$TASK_ROLE_NAME" &>/dev/null || \
    $AWS iam create-role --role-name "$TASK_ROLE_NAME" \
      --assume-role-policy-document "$TRUST" > /dev/null
  DYNAMO_SSM_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:BatchWriteItem", "dynamodb:PutItem"],
      "Resource": [
        "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$DYNAMO_TABLE",
        "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$METRICS_TABLE",
        "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$RECALL_TABLE"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "ssm:GetParameter",
      "Resource": "arn:aws:ssm:$AWS_REGION:$ACCOUNT_ID:parameter$SSM_FLAG_PATH"
    }
  ]
}
POLICY
)
  $AWS iam put-role-policy --role-name "$TASK_ROLE_NAME" \
    --policy-name "redrumWriterPolicy" \
    --policy-document "$DYNAMO_SSM_POLICY" > /dev/null

  # querier task role — needs to write to metrics table
  QUERIER_ROLE_NAME="redrumQuerierTaskRole"
  $AWS iam get-role --role-name "$QUERIER_ROLE_NAME" &>/dev/null || \
    $AWS iam create-role --role-name "$QUERIER_ROLE_NAME" \
      --assume-role-policy-document "$TRUST" > /dev/null
  QUERIER_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["dynamodb:PutItem"],
    "Resource": "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$METRICS_TABLE"
  }]
}
POLICY
)
  $AWS iam put-role-policy --role-name "$QUERIER_ROLE_NAME" \
    --policy-name "redrumQuerierPolicy" \
    --policy-document "$QUERIER_POLICY" > /dev/null
  QUERIER_TASK_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$QUERIER_ROLE_NAME"

  TASK_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$TASK_ROLE_NAME"
  ok "Task roles ready"

  # --- Lambda execution role ---
  info "Setting up Lambda execution role..."
  LAMBDA_ROLE_NAME="redrumLambdaRole"
  LAMBDA_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  $AWS iam get-role --role-name "$LAMBDA_ROLE_NAME" &>/dev/null || \
    $AWS iam create-role --role-name "$LAMBDA_ROLE_NAME" \
      --assume-role-policy-document "$LAMBDA_TRUST" > /dev/null
  $AWS iam attach-role-policy --role-name "$LAMBDA_ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" 2>/dev/null || true

  TABLE_STREAM_ARN=$($AWS dynamodb describe-table --table-name "$DYNAMO_TABLE" \
    --query 'Table.LatestStreamArn' --output text)
  LAMBDA_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:UpdateItem"],
      "Resource": "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$DYNAMO_TABLE"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:Scan", "dynamodb:UpdateItem"],
      "Resource": "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$RECALL_TABLE"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$STATS_TABLE"
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetRecords", "dynamodb:GetShardIterator",
        "dynamodb:DescribeStream", "dynamodb:ListStreams"
      ],
      "Resource": "$TABLE_STREAM_ARN"
    }
  ]
}
POLICY
)
  $AWS iam put-role-policy --role-name "$LAMBDA_ROLE_NAME" \
    --policy-name "redrumLambdaPolicy" \
    --policy-document "$LAMBDA_POLICY" > /dev/null
  LAMBDA_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$LAMBDA_ROLE_NAME"
  ok "Lambda role: $LAMBDA_ROLE_ARN"

  # --- Deploy tracker Lambda ---
  info "Deploying tracker Lambda..."
  TRACKER_IMAGE="$ECR_BASE/redrum-tracker:latest"

  EXISTING_LAMBDA=$($AWS lambda get-function --function-name "$LAMBDA_FUNCTION" \
    --query 'Configuration.FunctionName' --output text 2>/dev/null || true)

  if [[ -n "$EXISTING_LAMBDA" ]]; then
    $AWS lambda update-function-code \
      --function-name "$LAMBDA_FUNCTION" \
      --image-uri "$TRACKER_IMAGE" > /dev/null
    # wait for code update to finish before changing config
    for i in $(seq 1 20); do
      STATUS=$($AWS lambda get-function --function-name "$LAMBDA_FUNCTION" \
        --query 'Configuration.LastUpdateStatus' --output text 2>/dev/null || echo "InProgress")
      [[ "$STATUS" == "Successful" ]] && break
      sleep 5
    done
    $AWS lambda update-function-configuration \
      --function-name "$LAMBDA_FUNCTION" \
      --timeout "$TRACKER_TIMEOUT" > /dev/null
    ok "Updated Lambda: $LAMBDA_FUNCTION"
  else
    # IAM propagation can take ~15s — retry until the role is assumable
    info "Creating Lambda (retrying until IAM role is assumable)..."
    for attempt in $(seq 1 10); do
      if $AWS lambda create-function \
          --function-name "$LAMBDA_FUNCTION" \
          --package-type Image \
          --code ImageUri="$TRACKER_IMAGE" \
          --role "$LAMBDA_ROLE_ARN" \
          --timeout "$TRACKER_TIMEOUT" \
          --memory-size 256 \
          --environment "Variables={INDEX_HOST=$INDEX_HOST,PINECONE_API_KEY=$PINECONE_API_KEY,DYNAMO_TABLE=$DYNAMO_TABLE}" \
          > /dev/null 2>&1; then
        break
      fi
      echo "  attempt $attempt/10 — IAM not ready yet, waiting 10s..."
      sleep 10
    done
    ok "Created Lambda: $LAMBDA_FUNCTION"

    # wait for Lambda to be active before adding trigger
    info "Waiting for Lambda to become active..."
    for i in $(seq 1 20); do
      STATE=$($AWS lambda get-function --function-name "$LAMBDA_FUNCTION" \
        --query 'Configuration.State' --output text 2>/dev/null || echo "Pending")
      [[ "$STATE" == "Active" ]] && break
      echo "  state=$STATE, waiting 5s..."
      sleep 5
    done

    # wire DynamoDB Stream → Lambda
    $AWS lambda create-event-source-mapping \
      --function-name "$LAMBDA_FUNCTION" \
      --event-source-arn "$TABLE_STREAM_ARN" \
      --starting-position LATEST \
      --batch-size 10 \
      --bisect-batch-on-function-error \
      > /dev/null
    ok "DynamoDB Stream → Lambda trigger created"
  fi

  # --- recall Lambda ---
  info "Deploying recall Lambda..."
  RECALL_IMAGE="$ECR_BASE/redrum-recall:latest"
  RECALL_ENV="Variables={INDEX_HOST=$INDEX_HOST,PINECONE_API_KEY=$PINECONE_API_KEY,RECALL_TABLE=$RECALL_TABLE,TOP_K=$TOP_K}"
  EXISTING_RECALL=$($AWS lambda get-function --function-name "redrum-recall" \
    --query 'Configuration.FunctionName' --output text 2>/dev/null || true)
  if [[ -n "$EXISTING_RECALL" ]]; then
    $AWS lambda update-function-code --function-name "redrum-recall" --image-uri "$RECALL_IMAGE" > /dev/null
    for i in $(seq 1 12); do
      STATUS=$($AWS lambda get-function --function-name "redrum-recall" \
        --query 'Configuration.LastUpdateStatus' --output text 2>/dev/null || echo "InProgress")
      [[ "$STATUS" == "Successful" ]] && break; sleep 5
    done
    $AWS lambda update-function-configuration --function-name "redrum-recall" \
      --timeout "$RECALL_TIMEOUT" --environment "$RECALL_ENV" > /dev/null
    ok "Updated Lambda: redrum-recall"
  else
    for attempt in $(seq 1 10); do
      $AWS lambda create-function \
        --function-name "redrum-recall" \
        --package-type Image \
        --code ImageUri="$RECALL_IMAGE" \
        --role "$LAMBDA_ROLE_ARN" \
        --timeout "$RECALL_TIMEOUT" \
        --memory-size 256 \
        --environment "$RECALL_ENV" \
        > /dev/null 2>&1 && break
      echo "  attempt $attempt/10 — waiting 10s..."; sleep 10
    done
    ok "Created Lambda: redrum-recall"
    for i in $(seq 1 20); do
      STATE=$($AWS lambda get-function --function-name "redrum-recall" \
        --query 'Configuration.State' --output text 2>/dev/null || echo "Pending")
      [[ "$STATE" == "Active" ]] && break; echo "  state=$STATE, waiting 5s..."; sleep 5
    done
    # EventBridge rule — every 2 minutes
    RECALL_RULE_ARN=$($AWS events put-rule \
      --name "redrum-recall-schedule" \
      --schedule-expression "rate(2 minutes)" \
      --state ENABLED \
      --query 'RuleArn' --output text)
    RECALL_LAMBDA_ARN=$($AWS lambda get-function --function-name "redrum-recall" \
      --query 'Configuration.FunctionArn' --output text)
    $AWS lambda add-permission --function-name "redrum-recall" \
      --statement-id "redrum-recall-event" \
      --action "lambda:InvokeFunction" \
      --principal "events.amazonaws.com" \
      --source-arn "$RECALL_RULE_ARN" > /dev/null 2>&1 || true
    $AWS events put-targets --rule "redrum-recall-schedule" \
      --targets "Id=1,Arn=$RECALL_LAMBDA_ARN" > /dev/null
    ok "EventBridge → redrum-recall (every 2 min)"
  fi

  # --- index-stats Lambda ---
  info "Deploying index-stats Lambda..."
  STATS_IMAGE="$ECR_BASE/redrum-index-stats:latest"
  STATS_ENV="Variables={INDEX_HOST=$INDEX_HOST,PINECONE_API_KEY=$PINECONE_API_KEY,STATS_TABLE=$STATS_TABLE}"
  EXISTING_STATS=$($AWS lambda get-function --function-name "redrum-index-stats" \
    --query 'Configuration.FunctionName' --output text 2>/dev/null || true)
  if [[ -n "$EXISTING_STATS" ]]; then
    $AWS lambda update-function-code --function-name "redrum-index-stats" --image-uri "$STATS_IMAGE" > /dev/null
    for i in $(seq 1 12); do
      STATUS=$($AWS lambda get-function --function-name "redrum-index-stats" \
        --query 'Configuration.LastUpdateStatus' --output text 2>/dev/null || echo "InProgress")
      [[ "$STATUS" == "Successful" ]] && break; sleep 5
    done
    $AWS lambda update-function-configuration --function-name "redrum-index-stats" \
      --timeout "$STATS_TIMEOUT" --environment "$STATS_ENV" > /dev/null
    ok "Updated Lambda: redrum-index-stats"
  else
    for attempt in $(seq 1 10); do
      $AWS lambda create-function \
        --function-name "redrum-index-stats" \
        --package-type Image \
        --code ImageUri="$STATS_IMAGE" \
        --role "$LAMBDA_ROLE_ARN" \
        --timeout "$STATS_TIMEOUT" \
        --memory-size 128 \
        --environment "$STATS_ENV" \
        > /dev/null 2>&1 && break
      echo "  attempt $attempt/10 — waiting 10s..."; sleep 10
    done
    ok "Created Lambda: redrum-index-stats"
    for i in $(seq 1 20); do
      STATE=$($AWS lambda get-function --function-name "redrum-index-stats" \
        --query 'Configuration.State' --output text 2>/dev/null || echo "Pending")
      [[ "$STATE" == "Active" ]] && break; echo "  state=$STATE, waiting 5s..."; sleep 5
    done
    # EventBridge rule — every 1 minute
    STATS_RULE_ARN=$($AWS events put-rule \
      --name "redrum-index-stats-schedule" \
      --schedule-expression "rate(1 minute)" \
      --state ENABLED \
      --query 'RuleArn' --output text)
    STATS_LAMBDA_ARN=$($AWS lambda get-function --function-name "redrum-index-stats" \
      --query 'Configuration.FunctionArn' --output text)
    $AWS lambda add-permission --function-name "redrum-index-stats" \
      --statement-id "redrum-index-stats-event" \
      --action "lambda:InvokeFunction" \
      --principal "events.amazonaws.com" \
      --source-arn "$STATS_RULE_ARN" > /dev/null 2>&1 || true
    $AWS events put-targets --rule "redrum-index-stats-schedule" \
      --targets "Id=1,Arn=$STATS_LAMBDA_ARN" > /dev/null
    ok "EventBridge → redrum-index-stats (every 1 min)"
  fi

  # --- ECS cluster ---
  info "Creating ECS cluster $CLUSTER..."
  $AWS ecs create-cluster --cluster-name "$CLUSTER" > /dev/null 2>&1 || true
  ok "Cluster ready"

  # --- networking: use default VPC ---
  info "Resolving default VPC networking..."
  DEFAULT_VPC=$($AWS ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
  SUBNETS=$($AWS ec2 describe-subnets \
    --filters "Name=vpc-id,Values=$DEFAULT_VPC" \
    --query 'Subnets[*].SubnetId' --output text | tr '\t' ',')
  ok "VPC=$DEFAULT_VPC subnets=$SUBNETS"

  # security group
  SG_NAME="redrum-tasks"
  SG_ID=$($AWS ec2 describe-security-groups \
    --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$DEFAULT_VPC" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")
  if [[ "$SG_ID" == "None" || -z "$SG_ID" ]]; then
    SG_ID=$($AWS ec2 create-security-group \
      --group-name "$SG_NAME" \
      --description "redrum ECS tasks - outbound only" \
      --vpc-id "$DEFAULT_VPC" \
      --query 'GroupId' --output text)
    # allow all outbound (HTTPS to Pinecone), no inbound needed
    $AWS ec2 authorize-security-group-egress \
      --group-id "$SG_ID" \
      --protocol -1 --port -1 --cidr 0.0.0.0/0 2>/dev/null || true
  fi
  ok "Security group: $SG_ID"

  # --- register task definitions ---
  info "Registering ECS task definitions..."

  WRITER_ENV='[
    {"name":"INDEX_HOST",          "value":"'"$INDEX_HOST"'"},
    {"name":"PINECONE_API_KEY",    "value":"'"$PINECONE_API_KEY"'"},
    {"name":"AWS_REGION",          "value":"'"$AWS_REGION"'"},
    {"name":"VECTOR_DIM",          "value":"'"$VECTOR_DIM"'"},
    {"name":"WRITE_COUNT",         "value":"'"$WRITE_COUNT"'"},
    {"name":"MIN_SLEEP_SECONDS",   "value":"'"$MIN_SLEEP_SECONDS"'"},
    {"name":"MAX_SLEEP_SECONDS",   "value":"'"$MAX_SLEEP_SECONDS"'"},
    {"name":"DYNAMO_TABLE",        "value":"'"$DYNAMO_TABLE"'"},
    {"name":"METRICS_TABLE",       "value":"'"$METRICS_TABLE"'"},
    {"name":"RECALL_TABLE",        "value":"'"$RECALL_TABLE"'"},
    {"name":"SSM_FLAG_PATH",       "value":"'"$SSM_FLAG_PATH"'"}
  ]'

  QUERIER_ENV='[
    {"name":"INDEX_HOST",          "value":"'"$INDEX_HOST"'"},
    {"name":"PINECONE_API_KEY",    "value":"'"$PINECONE_API_KEY"'"},
    {"name":"AWS_REGION",          "value":"'"$AWS_REGION"'"},
    {"name":"VECTOR_DIM",          "value":"'"$VECTOR_DIM"'"},
    {"name":"QUERY_COUNT",         "value":"'"$QUERY_COUNT"'"},
    {"name":"TOP_K",               "value":"'"$TOP_K"'"},
    {"name":"MIN_SLEEP_SECONDS",   "value":"'"$MIN_SLEEP_SECONDS"'"},
    {"name":"MAX_SLEEP_SECONDS",   "value":"'"$MAX_SLEEP_SECONDS"'"},
    {"name":"METRICS_TABLE",       "value":"'"$METRICS_TABLE"'"}
  ]'

  for svc in writer querier; do
    if [[ "$svc" == "writer" ]]; then
      ENV_JSON="$WRITER_ENV"
      TASK_ROLE_FIELD='"taskRoleArn": "'"$TASK_ROLE_ARN"'",'
    else
      ENV_JSON="$QUERIER_ENV"
      TASK_ROLE_FIELD='"taskRoleArn": "'"$QUERIER_TASK_ROLE_ARN"'",'
    fi

    TASK_DEF=$(cat <<EOF
{
  "family": "redrum-$svc",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "$EXECUTION_ROLE_ARN",
  $TASK_ROLE_FIELD
  "containerDefinitions": [{
    "name": "redrum-$svc",
    "image": "$ECR_BASE/redrum-$svc:latest",
    "essential": true,
    "environment": $ENV_JSON,
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "$LOG_GROUP",
        "awslogs-region": "$AWS_REGION",
        "awslogs-stream-prefix": "$svc"
      }
    }
  }]
}
EOF
)
    $AWS ecs register-task-definition --cli-input-json "$TASK_DEF" > /dev/null
    ok "Task definition: redrum-$svc"
  done

  # --- create/update ECS services ---
  info "Starting ECS services..."
  NET_CONFIG="awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG_ID],assignPublicIp=ENABLED}"

  for svc in writer querier; do
    if [[ "$svc" == "writer" ]]; then COUNT="$WRITER_COUNT"; else COUNT="$QUERIER_COUNT"; fi
    EXISTS=$($AWS ecs describe-services --cluster "$CLUSTER" --services "redrum-$svc" \
      --query 'services[?status!=`INACTIVE`].serviceName' --output text 2>/dev/null || true)
    if [[ -n "$EXISTS" ]]; then
      $AWS ecs update-service \
        --cluster "$CLUSTER" \
        --service "redrum-$svc" \
        --task-definition "redrum-$svc" \
        --desired-count "$COUNT" > /dev/null
      ok "Updated service: redrum-$svc (count=$COUNT)"
    else
      $AWS ecs create-service \
        --cluster "$CLUSTER" \
        --service-name "redrum-$svc" \
        --task-definition "redrum-$svc" \
        --desired-count "$COUNT" \
        --launch-type FARGATE \
        --network-configuration "$NET_CONFIG" > /dev/null
      ok "Created service: redrum-$svc (count=$COUNT)"
    fi
  done

  echo ""
  ok "Deploy complete! Tasks are starting (may take ~30s)."
  echo ""
  echo "  Monitor:  ./redrum.sh logs"
  echo "  Status:   ./redrum.sh status"
  echo "  Kill:     ./redrum.sh kill"
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
cmd_status() {
  info "ECS service status (cluster: $CLUSTER)"
  echo ""
  for svc in writer querier; do
    echo "--- redrum-$svc ---"
    $AWS ecs describe-services \
      --cluster "$CLUSTER" \
      --services "redrum-$svc" \
      --query 'services[0].{Status:status,Running:runningCount,Desired:desiredCount,Pending:pendingCount,LastDeployment:deployments[0].updatedAt}' \
      --output table 2>/dev/null || echo "(not found)"
    echo ""
  done
}

# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------
cmd_logs() {
  TARGET="${1:-all}"
  info "Tailing logs from CloudWatch (Ctrl-C to stop)..."

  if [[ "$TARGET" == "writer" ]]; then
    $AWS logs tail "$LOG_GROUP" --log-stream-name-prefix "writer" --follow
  elif [[ "$TARGET" == "querier" ]]; then
    $AWS logs tail "$LOG_GROUP" --log-stream-name-prefix "querier" --follow
  else
    # tail both — interleaved
    $AWS logs tail "$LOG_GROUP" --follow
  fi
}

# ---------------------------------------------------------------------------
# kill
# ---------------------------------------------------------------------------
cmd_kill() {
  info "Stopping services (setting desired count to 0)..."
  for svc in writer querier; do
    $AWS ecs update-service --cluster "$CLUSTER" --service "redrum-$svc" --desired-count 0 > /dev/null \
      && ok "Stopped: redrum-$svc" \
      || echo "  (redrum-$svc not found, skipping)"
  done
  ok "Both services stopped. Run './redrum.sh deploy' to restart."
}

# ---------------------------------------------------------------------------
# destroy — full teardown
# ---------------------------------------------------------------------------
cmd_destroy() {
  info "Destroying all redrum AWS resources..."

  # stop and delete services
  for svc in writer querier; do
    $AWS ecs update-service --cluster "$CLUSTER" --service "redrum-$svc" --desired-count 0 > /dev/null 2>&1 || true
    $AWS ecs delete-service --cluster "$CLUSTER" --service "redrum-$svc" --force > /dev/null 2>&1 || true
    ok "Deleted service: redrum-$svc"
  done

  # delete cluster
  $AWS ecs delete-cluster --cluster "$CLUSTER" > /dev/null 2>&1 && ok "Deleted cluster: $CLUSTER" || true

  # delete ECR repos
  for repo in redrum-writer redrum-querier; do
    $AWS ecr delete-repository --repository-name "$repo" --force > /dev/null 2>&1 && ok "Deleted ECR: $repo" || true
  done

  # delete log group
  $AWS logs delete-log-group --log-group-name "$LOG_GROUP" > /dev/null 2>&1 && ok "Deleted log group: $LOG_GROUP" || true

  # delete Lambda + event source mapping
  $AWS lambda delete-function --function-name "$LAMBDA_FUNCTION" > /dev/null 2>&1 && ok "Deleted Lambda: $LAMBDA_FUNCTION" || true

  # delete DynamoDB table
  $AWS dynamodb delete-table --table-name "$DYNAMO_TABLE" > /dev/null 2>&1 && ok "Deleted DynamoDB table: $DYNAMO_TABLE" || true

  # delete SSM parameter
  $AWS ssm delete-parameter --name "$SSM_FLAG_PATH" > /dev/null 2>&1 && ok "Deleted SSM: $SSM_FLAG_PATH" || true

  for repo in redrum-tracker redrum-recall redrum-index-stats; do
    $AWS ecr delete-repository --repository-name "$repo" --force > /dev/null 2>&1 && ok "Deleted ECR: $repo" || true
  done

  # delete recall + index-stats Lambdas and EventBridge rules
  for fn in redrum-recall redrum-index-stats; do
    $AWS lambda delete-function --function-name "$fn" > /dev/null 2>&1 && ok "Deleted Lambda: $fn" || true
  done
  $AWS events remove-targets --rule "redrum-recall-schedule"       --ids "1" > /dev/null 2>&1 || true
  $AWS events remove-targets --rule "redrum-index-stats-schedule"  --ids "1" > /dev/null 2>&1 || true
  $AWS events delete-rule --name "redrum-recall-schedule"      > /dev/null 2>&1 || true
  $AWS events delete-rule --name "redrum-index-stats-schedule" > /dev/null 2>&1 || true
  ok "EventBridge rules removed"

  # delete extra DynamoDB tables
  for tbl in "$METRICS_TABLE" "$RECALL_TABLE" "$STATS_TABLE"; do
    $AWS dynamodb delete-table --table-name "$tbl" > /dev/null 2>&1 && ok "Deleted DynamoDB table: $tbl" || true
  done

  # deregister autoscaling
  for svc in writer querier; do
    $AWS application-autoscaling deregister-scalable-target \
      --service-namespace ecs \
      --scalable-dimension ecs:service:DesiredCount \
      --resource-id "service/$CLUSTER/redrum-$svc" > /dev/null 2>&1 || true
  done

  ok "All redrum resources destroyed."
}

# ---------------------------------------------------------------------------
# autoscale — register or remove CPU-based ECS scaling policies
# ---------------------------------------------------------------------------
cmd_autoscale() {
  MODE="${1:-enable}"
  if [[ "$MODE" == "enable" ]]; then
    info "Registering ECS auto-scaling (CPU target: out=${SCALE_OUT_CPU}% in=${SCALE_IN_CPU}%)..."
    for svc in writer querier; do
      if [[ "$svc" == "writer" ]]; then MIN=$WRITER_MIN; MAX=$WRITER_MAX
      else                              MIN=$QUERIER_MIN; MAX=$QUERIER_MAX; fi

      $AWS application-autoscaling register-scalable-target \
        --service-namespace ecs \
        --scalable-dimension ecs:service:DesiredCount \
        --resource-id "service/$CLUSTER/redrum-$svc" \
        --min-capacity "$MIN" \
        --max-capacity "$MAX" > /dev/null

      POLICY=$(cat <<EOF
{
  "TargetValue": $SCALE_OUT_CPU,
  "PredefinedMetricSpecification": {
    "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
  },
  "ScaleOutCooldown": 60,
  "ScaleInCooldown": 300
}
EOF
)
      $AWS application-autoscaling put-scaling-policy \
        --service-namespace ecs \
        --scalable-dimension ecs:service:DesiredCount \
        --resource-id "service/$CLUSTER/redrum-$svc" \
        --policy-name "redrum-$svc-cpu" \
        --policy-type TargetTrackingScaling \
        --target-tracking-scaling-policy-configuration "$POLICY" > /dev/null
      ok "Auto-scaling enabled: redrum-$svc (min=$MIN max=$MAX)"
    done

  elif [[ "$MODE" == "disable" ]]; then
    info "Removing ECS auto-scaling policies..."
    for svc in writer querier; do
      $AWS application-autoscaling delete-scaling-policy \
        --service-namespace ecs \
        --scalable-dimension ecs:service:DesiredCount \
        --resource-id "service/$CLUSTER/redrum-$svc" \
        --policy-name "redrum-$svc-cpu" > /dev/null 2>&1 || true
      $AWS application-autoscaling deregister-scalable-target \
        --service-namespace ecs \
        --scalable-dimension ecs:service:DesiredCount \
        --resource-id "service/$CLUSTER/redrum-$svc" > /dev/null 2>&1 || true
      ok "Auto-scaling disabled: redrum-$svc"
    done
  else
    err "Usage: $0 autoscale {enable|disable}"
  fi
}

# ---------------------------------------------------------------------------
# ramp — gradually step writer/querier counts to target over RAMP_STEPS steps
# ---------------------------------------------------------------------------
cmd_ramp() {
  TARGET_WRITER="${1:-}" ; TARGET_QUERIER="${2:-}"
  [[ -z "$TARGET_WRITER" || -z "$TARGET_QUERIER" ]] && \
    err "Usage: $0 ramp <writer_count> <querier_count>"

  CURRENT_WRITER=$($AWS ecs describe-services --cluster "$CLUSTER" --services "redrum-writer" \
    --query 'services[0].desiredCount' --output text 2>/dev/null || echo 0)
  CURRENT_QUERIER=$($AWS ecs describe-services --cluster "$CLUSTER" --services "redrum-querier" \
    --query 'services[0].desiredCount' --output text 2>/dev/null || echo 0)

  info "Ramping writer $CURRENT_WRITER→$TARGET_WRITER querier $CURRENT_QUERIER→$TARGET_QUERIER over $((RAMP_STEPS * RAMP_STEP_SECONDS))s..."

  for step in $(seq 1 "$RAMP_STEPS"); do
    W=$(( CURRENT_WRITER  + (TARGET_WRITER  - CURRENT_WRITER)  * step / RAMP_STEPS ))
    Q=$(( CURRENT_QUERIER + (TARGET_QUERIER - CURRENT_QUERIER) * step / RAMP_STEPS ))
    $AWS ecs update-service --cluster "$CLUSTER" --service "redrum-writer"  --desired-count "$W" > /dev/null
    $AWS ecs update-service --cluster "$CLUSTER" --service "redrum-querier" --desired-count "$Q" > /dev/null
    ok "Step $step/$RAMP_STEPS — writer=$W querier=$Q"
    [[ "$step" -lt "$RAMP_STEPS" ]] && sleep "$RAMP_STEP_SECONDS"
  done
  ok "Ramp complete."
}

# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------
case "${1:-}" in
  deploy)    cmd_deploy ;;
  status)    cmd_status ;;
  logs)      cmd_logs "${2:-all}" ;;
  kill)      cmd_kill ;;
  destroy)   cmd_destroy ;;
  autoscale) cmd_autoscale "${2:-enable}" ;;
  ramp)      cmd_ramp "${2:-}" "${3:-}" ;;
  *)
    echo "Usage: $0 {deploy|status|logs [writer|querier]|kill|destroy|autoscale [enable|disable]|ramp <writers> <queriers>}"
    exit 1
    ;;
esac

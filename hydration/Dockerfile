FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app
COPY requirements.txt .
RUN uv pip install --no-cache -r requirements.txt
COPY hydrator.py .

CMD ["python", "-u", "hydrator.py"]

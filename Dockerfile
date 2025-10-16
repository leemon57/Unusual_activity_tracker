
# Use an official lightweight Python image for build stage
FROM python:3.11-slim as base

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

# install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	gcc \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create a non-root user
RUN useradd --create-home appuser

# Copy only requirements first to leverage docker cache
COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip setuptools wheel && \
	pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Ensure files are owned by appuser
RUN chown -R appuser:appuser /app

USER appuser

# Expose a default port in case the app is a web service
EXPOSE 8080

# Simple healthcheck (will succeed if python can import the project)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import sys; import main; sys.exit(0)" || exit 1

# Default command: run main.py (adjust if your app uses a different entrypoint)
CMD ["python", "main.py"]

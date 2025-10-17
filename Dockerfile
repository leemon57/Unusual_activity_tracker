# Base Python image (Debian slim)
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install Python deps first to leverage Docker layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source (feeds, main.py, etc.)
COPY . .

# Ensure logs stream immediately
ENV PYTHONUNBUFFERED=1

# Default process
CMD ["python", "main.py"]
# Use official Python 3.12 slim image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies (optional: add more if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create and set working directory
WORKDIR /app

# Copy local code to the container
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip \
 && if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Default command (can be overridden in docker-compose or CMD args)
CMD ["python", "main.py"]

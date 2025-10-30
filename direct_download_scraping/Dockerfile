# Use official Python runtime as base image
FROM python:3.12-slim

# Set working directory in container
WORKDIR /app

# Install system dependencies (lxml requires libxml2, libxslt1, and build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 \
    libxml2-dev \
    libxslt1.1 \
    libxslt1-dev \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scraper scripts and configuration
COPY yb_direct_download_scraper.py .

# Create directories for output
RUN mkdir -p yearbook_downloads scraper_logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV LOG_FOLDER=/app/scraper_logs
ENV OUTPUT_FOLDER=/app/yearbook_downloads

# Volume mounts for data persistence
VOLUME ["/app/yearbook_downloads", "/app/scraper_logs"]

# Default command runs the scraper
CMD ["python", "yb_direct_download_scraper.py"]

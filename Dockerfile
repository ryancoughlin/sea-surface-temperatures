FROM python:3.12-slim

# Install system dependencies for NetCDF and Cartopy
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    proj-bin \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories with correct permissions
RUN mkdir -p /output && \
    chmod -R 777 /output

# Create a non-root user
RUN useradd -m appuser
USER appuser

# Command to run the script
CMD ["python", "main.py"]
FROM python:3.12-slim

# Install system dependencies for NetCDF and other required packages
RUN apt-get update && apt-get install -y \
    gcc \
    libhdf5-dev \
    libnetcdf-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories with correct permissions
RUN mkdir -p /data /output && \
    chmod -R 777 /data /output

# Create a non-root user
RUN useradd -m appuser
USER appuser

# Command to run the script
CMD ["python", "main.py"]

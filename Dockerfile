# Use prebuilt GDAL image with Python3.12
FROM osgeo/gdal:alpine-small-latest

# Install additional Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Set working directory
WORKDIR /

# Copy application code
COPY . .

# Create necessary directories with correct permissions
RUN mkdir -p /data /output && \
    chmod -R 777 /data /output

# Create a non-root user
RUN adduser -D appuser
USER appuser

# Command to run the script
CMD ["python", "main.py"]
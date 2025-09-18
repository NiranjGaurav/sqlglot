FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

# Install dependencies required for building certain packages
RUN apk add --no-cache gcc g++ cmake make libxml2-dev libxslt-dev openssl && \
    adduser --home /app e6 --disabled-password

# Copy the main requirements file from the root (includes automated_processing dependencies)
COPY requirements_bp.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements_bp.txt

# Install specific FastAPI, Uvicorn, and multipart dependencies
RUN pip install fastapi==0.115.6 uvicorn==0.32.0 python-multipart 

# Copy the entire sqlglot package (needed for imports in automated_api.py)
COPY sqlglot/ ./sqlglot/

# Copy the automated_processing directory
COPY automated_processing/ ./automated_processing/

# Copy the automated_api.py file
COPY automated_api.py .

# Copy other necessary files for the automated processing
COPY apis/ ./apis/
COPY log_collector.py .

# Make port 8101 available to the world outside this container
USER e6
EXPOSE 8101

HEALTHCHECK none

# Run the Automated API using Uvicorn
# Workers will be calculated dynamically based on CPU cores
CMD ["python", "automated_api.py"]
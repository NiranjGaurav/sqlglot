FROM python:3.12-alpine

# Set the working directory in the container
WORKDIR /app

# Install build dependencies, build packages, then remove build deps for smaller image
RUN apk add --no-cache \
    # Runtime dependencies (keep these)
    libxml2 libxslt openssl \
    # Build dependencies (will remove after pip install)
    && apk add --virtual .build-deps \
    gcc g++ cmake make \
    libxml2-dev libxslt-dev openssl-dev \
    linux-headers musl-dev python3-dev \
    && adduser --home /app e6 --disabled-password

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies and remove build dependencies in same layer
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install fastapi==0.115.4 uvicorn==0.32.0 python-multipart \
    && apk del .build-deps

# Copy the rest of the application code into the container
COPY . .

# Make port 8100 available to the world outside this container
USER e6
EXPOSE 8100

HEALTHCHECK none

# Run the FastAPI app using Uvicorn
# Workers will be calculated dynamically based on CPU cores
CMD ["python", "converter_api.py"]

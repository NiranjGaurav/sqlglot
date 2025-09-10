#!/bin/bash

# Kubernetes Deployment Script for SQLGlot System on Minikube
# This script deploys the FastAPI + Celery distributed processing system

set -e  # Exit on any error

echo "🚀 Starting SQLGlot Kubernetes deployment on Minikube..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if minikube is running
echo -e "${BLUE}📋 Checking Minikube status...${NC}"
if ! minikube status &>/dev/null; then
    echo -e "${YELLOW}⚠️  Minikube is not running. Starting Minikube...${NC}"
    minikube start --driver=docker --cpus=4 --memory=8192
    echo -e "${GREEN}✅ Minikube started successfully${NC}"
else
    echo -e "${GREEN}✅ Minikube is already running${NC}"
fi

# Set docker environment to use minikube's docker daemon
echo -e "${BLUE}🐳 Setting up Docker environment for Minikube...${NC}"
eval $(minikube docker-env)

# Build Docker images in Minikube's Docker environment
echo -e "${BLUE}🔨 Building Docker images...${NC}"

# Build FastAPI image
echo -e "${YELLOW}Building FastAPI image...${NC}"
docker build --no-cache -t sqlglot/api:latest -f Dockerfile .
echo -e "${GREEN}✅ FastAPI image built successfully${NC}"

# Build Celery Worker image  
echo -e "${YELLOW}Building Celery Worker image...${NC}"
docker build --no-cache -t sqlglot/worker:latest -f automated_processing/Dockerfile.worker .
echo -e "${GREEN}✅ Worker image built successfully${NC}"

# Build Frontend image
echo -e "${YELLOW}Building Frontend image...${NC}"
docker build --no-cache -t sqlglot/frontend:latest -f frontend/Dockerfile frontend/
echo -e "${GREEN}✅ Frontend image built successfully${NC}"

# Verify images are available
echo -e "${BLUE}📋 Verifying Docker images...${NC}"
docker images | grep sqlglot

# Apply Kubernetes manifests in order
echo -e "${BLUE}📦 Deploying to Kubernetes...${NC}"

# 1. Create namespace
echo -e "${YELLOW}Creating namespace...${NC}"
kubectl apply -f k8s/namespace.yaml

# 2. Apply ConfigMap and Secrets
echo -e "${YELLOW}Applying configuration...${NC}"
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secrets.yaml

# 3. Deploy Redis
echo -e "${YELLOW}Deploying Redis...${NC}"
kubectl apply -f k8s/redis.yaml

# Wait for Redis to be ready
echo -e "${BLUE}⏳ Waiting for Redis to be ready...${NC}"
kubectl wait --for=condition=ready pod -l app=redis -n sqlglot --timeout=120s

# 4. Deploy FastAPI
echo -e "${YELLOW}Deploying FastAPI service...${NC}"
kubectl apply -f k8s/fastapi.yaml

# 5. Deploy Celery Workers
echo -e "${YELLOW}Deploying Celery Workers (max 2)...${NC}"
kubectl apply -f k8s/workers.yaml

# 6. Deploy Frontend
echo -e "${YELLOW}Deploying Frontend...${NC}"
kubectl apply -f k8s/frontend.yaml

# Wait for deployments to be ready
echo -e "${BLUE}⏳ Waiting for deployments to be ready...${NC}"
kubectl wait --for=condition=available deployment/fastapi -n sqlglot --timeout=180s

kubectl wait --for=condition=available deployment/celery-workers -n sqlglot --timeout=180s
kubectl wait --for=condition=available deployment/frontend -n sqlglot --timeout=180s

# Get service URLs
echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo -e "${BLUE}📊 Deployment Status:${NC}"
kubectl get pods -n sqlglot
echo ""
kubectl get services -n sqlglot

# Get service URLs
FASTAPI_URL=$(minikube service fastapi -n sqlglot --url)
FRONTEND_URL=$(minikube service frontend -n sqlglot --url)
echo -e "${GREEN}🌐 FastAPI Service URL: ${FASTAPI_URL}${NC}"
echo -e "${GREEN}🔍 Health Check: ${FASTAPI_URL}/health${NC}"
echo -e "${GREEN}🎨 Frontend URL: ${FRONTEND_URL}${NC}"

# Show useful commands
echo -e "${BLUE}📋 Useful commands:${NC}"
echo "View pods:           kubectl get pods -n sqlglot"
echo "View services:       kubectl get services -n sqlglot"
echo "View logs (API):     kubectl logs -f deployment/fastapi -n sqlglot"
echo "View logs (Workers): kubectl logs -f deployment/celery-workers -n sqlglot"
echo "Scale workers:       kubectl scale deployment celery-workers --replicas=N -n sqlglot"
echo "Port forward:        kubectl port-forward service/fastapi 8080:8080 -n sqlglot"

# Test health endpoint
echo -e "${BLUE}🧪 Testing health endpoint...${NC}"
sleep 10  # Wait a bit for services to be fully ready
if curl -f "${FASTAPI_URL}/health" &>/dev/null; then
    echo -e "${GREEN}✅ Health check passed!${NC}"
else
    echo -e "${YELLOW}⚠️  Health check failed - service might still be starting${NC}"
fi

echo -e "${GREEN}🚀 SQLGlot system is now running on Kubernetes!${NC}"
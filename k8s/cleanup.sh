#!/bin/bash

# Kubernetes Cleanup Script for SQLGlot System
# This script removes all deployed resources

set -e

echo "🧹 Starting SQLGlot Kubernetes cleanup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📋 Removing Kubernetes resources...${NC}"

# Remove in reverse order of creation
echo -e "${YELLOW}Removing Celery Workers...${NC}"
kubectl delete -f k8s/workers.yaml --ignore-not-found=true

echo -e "${YELLOW}Removing FastAPI service...${NC}"
kubectl delete -f k8s/fastapi.yaml --ignore-not-found=true

echo -e "${YELLOW}Removing Redis...${NC}"
kubectl delete -f k8s/redis.yaml --ignore-not-found=true

echo -e "${YELLOW}Removing configuration...${NC}"
kubectl delete -f k8s/configmap.yaml --ignore-not-found=true
kubectl delete -f k8s/secrets.yaml --ignore-not-found=true

echo -e "${YELLOW}Removing namespace...${NC}"
kubectl delete -f k8s/namespace.yaml --ignore-not-found=true

# Remove Docker images (optional)
read -p "Do you want to remove Docker images? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}🐳 Removing Docker images...${NC}"
    eval $(minikube docker-env)
    docker rmi sqlglot/api:latest sqlglot/worker:latest --force || true
    echo -e "${GREEN}✅ Docker images removed${NC}"
fi

echo -e "${GREEN}🧹 Cleanup completed successfully!${NC}"
echo -e "${BLUE}📋 Remaining resources:${NC}"
kubectl get all -n sqlglot || echo "No resources found in sqlglot namespace"
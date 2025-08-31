#!/bin/bash

# SQLGlot Minikube Deployment Script
# Deploy SQLGlot system to local Minikube cluster using Helm charts

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

info() {
    echo -e "${BLUE}[MINIKUBE]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    command -v minikube >/dev/null 2>&1 || error "Minikube not installed"
    command -v kubectl >/dev/null 2>&1 || error "kubectl not installed"
    command -v helm >/dev/null 2>&1 || error "Helm not installed"
    command -v docker >/dev/null 2>&1 || error "Docker not installed"
    
    # Check if Minikube is running
    if ! minikube status >/dev/null 2>&1; then
        warn "Minikube is not running"
        return 1
    fi
    
    log "Prerequisites check passed ✓"
    return 0
}

# Start Minikube if not running
start_minikube() {
    info "Starting Minikube..."
    
    # Start with sufficient resources for the SQLGlot system
    minikube start \
        --cpus=4 \
        --memory=8192 \
        --disk-size=20g \
        --driver=docker
    
    # Enable required addons
    minikube addons enable ingress
    minikube addons enable storage-provisioner
    minikube addons enable default-storageclass
    
    log "Minikube started successfully ✓"
}

# Build Docker images in Minikube
build_images() {
    info "Building Docker images in Minikube environment..."
    
    # Use Minikube's Docker daemon
    eval $(minikube -p minikube docker-env)
    
    # Build API image (backend)
    log "Building API image..."
    docker build -t sqlglot-api:latest -f Dockerfile .
    
    # Build Worker image
    log "Building Worker image..."
    docker build -t sqlglot-worker:latest -f automated_processing/Dockerfile.worker .
    
    # Build Frontend image
    log "Building Frontend image..."
    docker build -t sqlglot-frontend:latest -f frontend/Dockerfile ./frontend
    
    log "All images built successfully ✓"
}

# Deploy using Helm
deploy_helm_charts() {
    info "Deploying Helm charts..."
    
    # Create namespace
    kubectl create namespace sqlglot --dry-run=client -o yaml | kubectl apply -f -
    
    # Deploy backend first (API + Workers + Redis)
    log "Deploying backend services..."
    helm upgrade --install sqlglot-backend \
        ./automated_processing/deployment \
        --namespace sqlglot \
        --wait \
        --timeout=300s
    
    # Deploy frontend
    log "Deploying frontend..."
    helm upgrade --install sqlglot-frontend \
        ./frontend/deployment \
        --namespace sqlglot \
        --wait \
        --timeout=300s
    
    log "Helm charts deployed successfully ✓"
}

# Show access information
show_access_info() {
    info "Getting access information..."
    
    # Get Minikube IP
    MINIKUBE_IP=$(minikube ip)
    
    # Get NodePort services
    FRONTEND_PORT=$(kubectl get svc sqlglot-frontend -n sqlglot -o jsonpath='{.spec.ports[0].nodePort}')
    BACKEND_PORT=$(kubectl get svc sqlglot-backend-api -n sqlglot -o jsonpath='{.spec.ports[0].nodePort}')
    
    echo ""
    info "🎉 Deployment completed successfully!"
    echo ""
    echo "📱 Frontend:    http://$MINIKUBE_IP:$FRONTEND_PORT"
    echo "🚀 Backend API: http://$MINIKUBE_IP:$BACKEND_PORT"
    echo "📊 Health:      http://$MINIKUBE_IP:$BACKEND_PORT/health"
    echo ""
    echo "Useful commands:"
    echo "  kubectl get pods -n sqlglot        # Check pod status"
    echo "  kubectl logs -f deployment/sqlglot-backend-api -n sqlglot  # API logs"
    echo "  kubectl logs -f deployment/sqlglot-backend-worker -n sqlglot  # Worker logs"
    echo "  minikube dashboard                  # Open Kubernetes dashboard"
}

# Clean up deployment
cleanup() {
    info "Cleaning up SQLGlot deployment..."
    
    # Uninstall Helm releases
    helm uninstall sqlglot-frontend -n sqlglot 2>/dev/null || true
    helm uninstall sqlglot-backend -n sqlglot 2>/dev/null || true
    
    # Delete namespace (this will delete all resources)
    kubectl delete namespace sqlglot 2>/dev/null || true
    
    log "Cleanup completed ✓"
}

# Show deployment status
show_status() {
    info "Deployment Status:"
    
    echo ""
    echo "Pods:"
    kubectl get pods -n sqlglot -o wide
    
    echo ""
    echo "Services:"
    kubectl get svc -n sqlglot
    
    echo ""
    echo "PVCs:"
    kubectl get pvc -n sqlglot
    
    # Check if services are responding
    MINIKUBE_IP=$(minikube ip)
    BACKEND_PORT=$(kubectl get svc sqlglot-backend-api -n sqlglot -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "N/A")
    
    if [ "$BACKEND_PORT" != "N/A" ]; then
        echo ""
        info "Health checks:"
        if curl -f -s "http://$MINIKUBE_IP:$BACKEND_PORT/health" >/dev/null; then
            log "✅ Backend API is healthy"
        else
            warn "❌ Backend API not responding"
        fi
    fi
}

# Show usage
show_usage() {
    echo "SQLGlot Minikube Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  deploy    Build images and deploy to Minikube"
    echo "  cleanup   Remove all SQLGlot resources from Minikube"
    echo "  status    Show current deployment status"
    echo "  build     Build Docker images only"
    echo "  start     Start Minikube with required configuration"
    echo "  logs      Show logs from all components"
    echo "  help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 deploy     # Full deployment"
    echo "  $0 status     # Check deployment status"
    echo "  $0 cleanup    # Remove deployment"
}

# Show logs
show_logs() {
    info "Showing logs from all components..."
    
    echo ""
    echo "=== API Logs ==="
    kubectl logs deployment/sqlglot-backend-api -n sqlglot --tail=50
    
    echo ""
    echo "=== Worker Logs ==="
    kubectl logs deployment/sqlglot-backend-worker -n sqlglot --tail=50
    
    echo ""
    echo "=== Frontend Logs ==="
    kubectl logs deployment/sqlglot-frontend -n sqlglot --tail=50
}

# Main script logic
case "${1:-help}" in
    deploy)
        if ! check_prerequisites; then
            start_minikube
        fi
        build_images
        deploy_helm_charts
        show_access_info
        ;;
    cleanup)
        cleanup
        ;;
    status)
        show_status
        ;;
    build)
        if ! check_prerequisites; then
            error "Minikube must be running to build images"
        fi
        build_images
        ;;
    start)
        start_minikube
        ;;
    logs)
        show_logs
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        error "Unknown command: $1"
        echo ""
        show_usage
        ;;
esac
#!/bin/bash

# Helper script for resolving upstream sync conflicts
# Usage: ./scripts/resolve-upstream-sync.sh [branch-name]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
UPSTREAM_REPO="https://github.com/tobymao/sqlglot.git"
UPSTREAM_BRANCH="main"

echo -e "${GREEN}🔄 Upstream Sync Conflict Resolution Helper${NC}"
echo "================================================"

# Get branch name from argument or generate new one
if [ -z "$1" ]; then
    BRANCH_NAME="rebase-$(date +%Y-%m-%d)"
    echo -e "${YELLOW}No branch name provided. Using: ${BRANCH_NAME}${NC}"
else
    BRANCH_NAME="$1"
fi

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo -e "\n${GREEN}Checking prerequisites...${NC}"
if ! command_exists git; then
    echo -e "${RED}Error: git is not installed${NC}"
    exit 1
fi

if ! command_exists python3; then
    echo -e "${RED}Error: python3 is not installed${NC}"
    exit 1
fi

# Function to handle errors
handle_error() {
    echo -e "\n${RED}❌ Error occurred: $1${NC}"
    echo -e "${YELLOW}Current status:${NC}"
    git status --short
    exit 1
}

# Trap errors
trap 'handle_error "Command failed"' ERR

# Step 1: Ensure we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}Error: Not in a git repository${NC}"
    exit 1
fi

# Step 2: Save current work
echo -e "\n${GREEN}Step 1: Checking for uncommitted changes...${NC}"
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    echo -e "${YELLOW}You have uncommitted changes. Stashing them...${NC}"
    git stash push -m "Auto-stash before upstream sync resolution"
    STASHED=true
else
    echo "Working directory is clean ✓"
    STASHED=false
fi

# Step 3: Fetch latest changes
echo -e "\n${GREEN}Step 2: Fetching latest changes...${NC}"
git fetch origin
git fetch upstream 2>/dev/null || {
    echo "Adding upstream remote..."
    git remote add upstream "$UPSTREAM_REPO"
    git fetch upstream
}

# Step 4: Create or checkout branch
echo -e "\n${GREEN}Step 3: Setting up branch ${BRANCH_NAME}...${NC}"
if git show-ref --verify --quiet refs/heads/"$BRANCH_NAME"; then
    echo "Branch exists. Checking out..."
    git checkout "$BRANCH_NAME"
else
    echo "Creating new branch from origin/main..."
    git checkout -b "$BRANCH_NAME" origin/main
fi

# Step 5: Attempt merge
echo -e "\n${GREEN}Step 4: Attempting to merge upstream/${UPSTREAM_BRANCH}...${NC}"
if git merge upstream/"$UPSTREAM_BRANCH" --no-edit; then
    echo -e "${GREEN}✅ Merge successful! No conflicts.${NC}"
    CONFLICTS=false
else
    echo -e "${YELLOW}⚠️ Merge conflicts detected!${NC}"
    CONFLICTS=true
fi

# Step 6: Handle conflicts
if [ "$CONFLICTS" = true ]; then
    echo -e "\n${YELLOW}Conflicted files:${NC}"
    git diff --name-only --diff-filter=U | while read -r file; do
        echo "  - $file"
    done

    echo -e "\n${GREEN}Step 5: Conflict Resolution${NC}"
    echo "Please resolve conflicts in the files listed above."
    echo ""
    echo "Options:"
    echo "  1) Open your favorite editor and resolve conflicts manually"
    echo "  2) Use 'git mergetool' to resolve conflicts"
    echo "  3) Abort this process with 'git merge --abort'"
    echo ""
    echo -e "${YELLOW}After resolving conflicts:${NC}"
    echo "  1. Stage resolved files: git add <file>"
    echo "  2. Complete merge: git commit"
    echo "  3. Run tests: make check"
    echo "  4. Push branch: git push origin ${BRANCH_NAME}"
    echo ""
    read -p "Press Enter to open VS Code (or Ctrl+C to resolve manually)..."

    # Try to open in VS Code if available
    if command_exists code; then
        code .
        echo -e "${GREEN}Opened in VS Code. Resolve conflicts and then continue...${NC}"
    fi

    # Wait for user to resolve
    echo ""
    read -p "Press Enter after resolving all conflicts..."

    # Check if conflicts are resolved
    if git diff --check; then
        echo -e "${GREEN}Conflicts appear to be resolved!${NC}"
        git add -A
        git commit --no-edit
    else
        echo -e "${RED}Conflicts still exist. Please resolve them first.${NC}"
        exit 1
    fi
fi

# Step 7: Run tests
echo -e "\n${GREEN}Step 6: Running tests...${NC}"
echo "Installing dependencies..."
make install-dev > /dev/null 2>&1 || {
    echo -e "${YELLOW}Warning: Could not install dev dependencies${NC}"
}

echo "Running make check..."
if make check; then
    echo -e "${GREEN}✅ All tests passed!${NC}"
    TESTS_PASSED=true
else
    echo -e "${YELLOW}⚠️ Some tests failed!${NC}"
    TESTS_PASSED=false
    echo ""
    read -p "Do you want to continue anyway? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Step 8: Push branch
echo -e "\n${GREEN}Step 7: Pushing branch...${NC}"
git push origin "$BRANCH_NAME" --force-with-lease || git push origin "$BRANCH_NAME"

# Step 9: Create PR
echo -e "\n${GREEN}Step 8: Creating Pull Request...${NC}"
echo "Branch ${BRANCH_NAME} has been pushed successfully!"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo "1. Go to: https://github.com/$(git config --get remote.origin.url | sed 's/.*github.com[:\/]\(.*\)\.git/\1/')/pull/new/${BRANCH_NAME}"
echo "2. Create a Pull Request to your company's main branch"
echo "3. Use this PR template:"
echo ""
echo "--- PR Template ---"
echo "Title: [Upstream Sync] Merge latest changes from tobymao/sqlglot - $(date +%Y-%m-%d)"
echo ""
echo "## Description"
echo "Weekly sync with upstream repository to incorporate latest changes."
echo ""
echo "## Changes"
echo "- Merged latest changes from upstream/main"
echo "- Resolved conflicts (if any)"
echo "- All tests passing: ${TESTS_PASSED}"
echo ""
echo "## Testing"
echo "- [x] make check passes"
echo "- [ ] Manual testing completed"
echo "- [ ] No breaking changes for our custom code"
echo "-------------------"

# Step 10: Restore stashed changes if any
if [ "$STASHED" = true ]; then
    echo -e "\n${GREEN}Restoring stashed changes...${NC}"
    git stash pop
fi

echo -e "\n${GREEN}🎉 Done! Your branch is ready for PR.${NC}"
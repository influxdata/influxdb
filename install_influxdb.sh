#!/bin/sh -e

################################################################################
# InfluxDB 3 Installation Script
################################################################################
#
# PURPOSE:
#   Automated setup script for InfluxDB 3 with intelligent installation method
#   selection and environment-aware configuration management. This script is
#   designed to be run for quick installation and non-production evaluation.
#
# INSTALLATION METHODS:
#   1. Docker Compose: Complete stack (InfluxDB + Explorer UI)
#      - Installs latest Docker images
#      - Auto-creates docker-compose.yml with proper networking
#      - Manages Explorer configuration and session secrets
#      - Supports upgrades of existing Docker installations
#
#   2. Binary Installation: Direct binary download with optional startup
#      - Downloads precompiled binaries for supported architectures
#      - Extracts to user home directory (~/.influxdb)
#      - Auto-configures shell PATH environment
#      - Offers Quick Start, Custom, or Install-Only modes
#
# DIRECTORY STRUCTURE:
#   Shared data directory (both installation methods):
#     ~/.influxdb/
#       ├── data/                  (Database files - shared between Docker & Binary)
#       └── plugins/               (Custom plugins - shared between Docker & Binary)
#
#   Docker Compose specific:
#     ~/.influxdb/docker/
#       ├── docker-compose.yml     (Docker service definitions)
#       ├── .env                   (Environment variables)
#       └── explorer/
#           ├── db/                (Explorer database)
#           └── config/            (Explorer configuration)
#
#   Binary specific:
#     ~/.influxdb/
#       ├── influxdb3              (Main binary)
#       └── logs/                  (Timestamped server logs)
#
# REQUIREMENTS/PREREQUISITES:
#   Core Requirements:
#     - POSIX-compliant shell (sh or bash)
#     - curl (for downloading binaries and verification)
#     - tar (for extracting archives)
#     - shasum (macOS) or sha256sum (Linux) for SHA256 verification
#
#   Docker Compose Method:
#     - Docker engine must be running and responding
#     - docker and docker compose commands available
#
#   Binary Method:
#     - Supported OS: macOS (ARM64 only), Linux (x86_64 or ARM64)
#     - Port availability (default 8181, auto-adjusts if in use)
#
# EXISTING INSTALLATION HANDLING:
#   Docker Compose:
#     - Detects existing docker-compose.yml at ~/.influxdb/docker/
#     - Automatically upgrades in-place when detected
#     - Extracts and reuses existing port configuration from .env
#     - Pulls latest images and restarts containers with health checks
#     - Preserves all data in bind-mounted directories
#
#   Binary:
#     - Overwrites any existing binary at ~/.influxdb/influxdb3
#     - Preserves data directory (~/.influxdb/data) automatically
#     - User can run script repeatedly to upgrade to latest version
#
# CONFIGURATION OPTIONS:
#   Command Line Arguments:
#     [enterprise]        Install Enterprise edition (default: Core)
#     --version VERSION   Specify InfluxDB version (default: 3.8.0)
#
#   Interactive Prompts (Binary Installation):
#     Installation Type:  Docker Compose or Binary
#     Startup Mode:       Quick Start, Custom, or Skip
#     Node ID:            Identifier for this server instance
#     Cluster ID:         (Enterprise only) Cluster identifier
#     Storage Type:       File, S3, Azure, GCS, or Memory
#     Cloud Credentials:  (If object storage selected)
#     License Type:       (Enterprise only) Trial or Home
#     License Email:      (Enterprise only) Email for activation
#
#   Docker Compose Specific:
#     Port Selection:     Automatic via find_next_available_port()
#     Session Secret:     Generated via openssl or date hash
#     InfluxDB Port:      Default 8181 (mapped to container 8181)
#     Explorer Port:      Default 8888 (mapped to container 80)
#     Container Restart:  unless-stopped policy
#
# EXIT POINTS AND CONDITIONS:
#   Successful Exits (exit 0):
#     - Docker Compose: After successful deployment with access points shown
#     - Docker Upgrade: After successful image pull and container restart
#     - Binary: After showing "Next Steps" information
#     - Skip Startup: After installation without starting service
#
#   Error Exits (exit 1):
#     - Unsupported OS/Architecture (Intel Mac, Windows, etc.)
#     - Docker not running or unavailable (Docker Compose path)
#     - Failed image pulls or docker compose commands
#     - Failed binary download, signature verification, or extraction
#     - Invalid SHA256 checksum on downloaded binary
#     - Container startup timeout (>60 seconds for InfluxDB/Explorer)
#     - Port allocation failure or port range exhausted
#
# SPECIAL BEHAVIORS:
#   Binary Download:
#     - URL format: https://dl.influxdata.com/influxdb/releases/
#       influxdb3-{edition_tag}-{version}_{artifact}.tar.gz
#     - Downloads corresponding .sha256 file for verification
#     - Validates checksums before extraction
#     - Automatically adds installation path to shell rc file
#
#   Docker Compose:
#     - Creates internal network (influxdb-network) for service communication
#     - Configures Explorer to communicate via container name internally
#     - Opens Explorer UI in default browser upon successful startup
#     - Filters verbose docker compose output for cleaner display
#
#   Health Checking:
#     - InfluxDB (Docker): Waits for "startup time:" in container logs
#     - Explorer: Polls /api/health endpoint for "ok/status/healthy"
#
# SHELL EXECUTION CONTEXT:
#   - Runs with `sh -e` (error exit on command failure)
#   - Uses POSIX-compatible syntax for maximum portability
#   - Disables shellcheck SC2059 for printf variable interpolation
#   - Exports INFLUXDB3_SERVE_INVOCATION_METHOD for telemetry tracking
#
################################################################################

# ==============================================================================
# SECTION 1: SCRIPT CONFIGURATION
# ==============================================================================

export LC_ALL=C

readonly GREEN='\033[0;32m'
readonly BLUE='\033[0;34m'
readonly BOLD='\033[1m'
readonly BOLDGREEN='\033[1;32m'
readonly DIM='\033[2m'
readonly YELLOW='\033[1;33m'
readonly RED='\033[0;31m'
readonly NC='\033[0m' # No Color

# No diagnostics for: 'printf "...${FOO}"'
# shellcheck disable=SC2059

# Docker Compose Constants
INFLUXDB_PORT=8181  # Can be changed if port is in use
EXPLORER_PORT=8888  # Can be changed if port is in use
readonly EXPLORER_IMAGE="influxdata/influxdb3-ui"
readonly MANUAL_TOKEN_MSG="MANUAL_TOKEN_CREATION_REQUIRED"

ARCHITECTURE=$(uname -m)
ARTIFACT=""
OS=""
INSTALL_LOC=~/.influxdb
BINARY_NAME="influxdb3"
PORT=8181

# Set the default (latest) version here. Users may specify a version using the
# --version arg (handled below)
INFLUXDB_VERSION="3.8.0"
EDITION="Core"
EDITION_TAG="core"

# ==============================================================================
# SECTION 2: COMMAND LINE ARGUMENT PARSING
# ==============================================================================

while [ $# -gt 0 ]; do
    case "$1" in
        --version)
            INFLUXDB_VERSION="$2"
            shift 2
            ;;
        enterprise)
            EDITION="Enterprise"
            EDITION_TAG="enterprise"
            shift 1
            ;;
        *)
            printf "Usage: %s [enterprise] [--version VERSION]\n" "$0"
            printf "  enterprise: Install the Enterprise edition (optional)\n"
            printf "  --version VERSION: Specify InfluxDB version (default: %s)\n" "$INFLUXDB_VERSION"
            exit 1
            ;;
    esac
done

# ==============================================================================
# SECTION 3: SYSTEM DETECTION
# ==============================================================================

case "$(uname -s)" in
    Linux*)     OS="Linux";;
    Darwin*)    OS="Darwin";;
    *)         OS="UNKNOWN";;
esac

if [ "${OS}" = "Linux" ]; then
    # ldd is a shell script but on some systems (eg Ubuntu) security hardening
    # prevents it from running when invoked directly. Since we only want to
    # use '--verbose', find the path to ldd, then invoke under sh to bypass ldd
    # hardening.
    if [ "${ARCHITECTURE}" = "x86_64" ] || [ "${ARCHITECTURE}" = "amd64" ]; then
        ARTIFACT="linux_amd64"
    elif [ "${ARCHITECTURE}" = "aarch64" ] || [ "${ARCHITECTURE}" = "arm64" ]; then
        ARTIFACT="linux_arm64"
    fi
elif [ "${OS}" = "Darwin" ]; then
    if [ "${ARCHITECTURE}" = "x86_64" ]; then
        printf "Intel Mac support is coming soon!\n"
        printf "Visit our public Discord at \033[4;94mhttps://discord.gg/az4jPm8x${NC} for additional guidance.\n"
        printf "View alternative binaries on our Getting Started guide at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/${NC}.\n"
        exit 1
    else
        ARTIFACT="darwin_arm64"
    fi
fi

# Exit if unsupported system
[ -n "${ARTIFACT}" ] || {
    printf "Unfortunately this script doesn't support your '${OS}' | '${ARCHITECTURE}' setup, or was unable to identify it correctly.\n"
    printf "Visit our public Discord at \033[4;94mhttps://discord.gg/az4jPm8x${NC} for additional guidance.\n"
    printf "View alternative binaries on our Getting Started guide at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/${NC}.\n"
    exit 1
}

URL="https://dl.influxdata.com/influxdb/releases/influxdb3-${EDITION_TAG}-${INFLUXDB_VERSION}_${ARTIFACT}.tar.gz"



# ==============================================================================
# SECTION 4: GENERAL UTILITY FUNCTIONS
# ==============================================================================
# Cross-cutting utilities used by multiple installation methods

# --- Port Management ---

# Find next available port starting from given port
# Parameters: $1 - starting_port (e.g., 8181)
# Returns: Available port number (echoes to stdout)
# Exits: 1 if port range exhausted (>32767)
find_next_available_port() {
    current_port="$1"

    lsof_exec=$(command -v lsof)
    if [ -z "$lsof_exec" ]; then
        echo "$current_port"
        return 0
    fi

    while "$lsof_exec" -i:"$current_port" -t >/dev/null 2>&1; do
        printf "├─${DIM} Port %s is in use. Finding new port.${NC}\n" "$current_port" >&2
        current_port=$((current_port + 1))
        if [ "$current_port" -gt 32767 ]; then
            printf "└─${RED} Could not find an available port. Aborting.${NC}\n" >&2
            exit 1
        fi
    done

    echo "$current_port"
}

# --- Browser Integration ---

# Utility function to open URL in browser
open_browser_url() {
    URL="$1"
    if command -v open >/dev/null 2>&1; then
        open "$URL" >/dev/null 2>&1 &
    elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open "$URL" >/dev/null 2>&1 &
    elif command -v start >/dev/null 2>&1; then
        start "$URL" >/dev/null 2>&1 &
    fi
}

# --- Security Utilities ---

# Utility function to generate session secret
generate_session_secret() {
    if [ "${OS}" = "Darwin" ]; then
        head -c 64 /dev/urandom | shasum -a 256 | cut -d ' ' -f 1
    else
        head -c 64 /dev/urandom | sha256sum | cut -d ' ' -f 1
    fi
}

# ==============================================================================
# SECTION 5: DOCKER COMPOSE UTILITIES
# ==============================================================================
# Docker-specific operations and helpers

# --- Docker Validation ---

# Function to check if Docker is available and running
check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        return 1
    fi
    if ! docker info >/dev/null 2>&1; then
        return 1
    fi
    if ! docker compose version >/dev/null 2>&1; then
        return 1
    fi
    return 0
}

# --- Image Management ---

# Pull Docker Compose image with configurable error handling
# Parameters:
#   $1 - service_name: Docker Compose service name to pull
#   $2 - display_name: Human-readable name for progress messages
#   $3 - is_fatal: "true" if failure should abort, "false" to continue (default: false)
# Returns: 0 on success or non-fatal failure, 1 on fatal failure
pull_docker_image() {
    service_name="$1"
    display_name="$2"
    is_fatal="${3:-false}"

    printf "├─ Pulling %s image..." "$display_name"

    # Capture error output
    PULL_OUTPUT=$(docker compose pull "$service_name" 2>&1)
    PULL_EXIT_CODE=$?

    if [ $PULL_EXIT_CODE -eq 0 ]; then
        printf "SUCCESS\n"
        return 0
    else
        if [ "$is_fatal" = "true" ]; then
            printf "${RED}FAILED${NC}\n"
            printf "└─ ${RED}Error: Failed to pull %s image${NC}\n" "$display_name"

            # Display actual error with context
            printf "\n${BOLD}Error details:${NC}\n"
            printf "%s\n\n" "$PULL_OUTPUT"

            # Provide specific troubleshooting based on error content
            printf "${BOLD}Possible causes:${NC}\n"
            if echo "$PULL_OUTPUT" | grep -qi "denied\|unauthorized"; then
                printf "  • Docker Hub authentication required or rate limited\n"
                printf "  • Try: docker login\n"
            elif echo "$PULL_OUTPUT" | grep -qi "network\|timeout\|connection"; then
                printf "  • Network connectivity issue\n"
                printf "  • Check your internet connection\n"
            elif echo "$PULL_OUTPUT" | grep -qi "daemon"; then
                printf "  • Docker daemon may not be responding\n"
                printf "  • Try: docker info\n"
            elif echo "$PULL_OUTPUT" | grep -qi "yaml\|control characters"; then
                printf "  • Invalid YAML syntax in docker-compose.yml\n"
                printf "  • Inspect file: cat %s/docker-compose.yml\n" "$DOCKER_DIR"
            else
                printf "  • Check Docker daemon status: docker info\n"
                printf "  • Check internet connection\n"
                printf "  • Check Docker Hub access\n"
            fi
            printf "\n"
            return 1
        else
            printf "${YELLOW}FAILED${NC}\n"
            printf "│  ${YELLOW}Warning: %s will not be available${NC}\n" "$display_name"
            printf "│  You can run this script again later to install it\n"
            return 0
        fi
    fi
}

# --- Output Formatting ---

# Utility function to filter Docker Compose output
filter_docker_output() {
    grep -v "version.*obsolete" | \
    grep -v "Creating$" | \
    grep -v "Created$" | \
    grep -v "Starting$" | \
    grep -v "Started$" | \
    grep -v "Running$" \
    || true
}

# --- Filesystem Setup ---

# Utility function to create docker directories with permissions
create_docker_directories() {
    DOCKER_DIR="$1"

    # Create shared data directories (used by both Docker and binary)
    mkdir -p "$HOME/.influxdb/data"
    mkdir -p "$HOME/.influxdb/plugins"

    # Create Docker-specific directories
    mkdir -p "$DOCKER_DIR/explorer/db"
    mkdir -p "$DOCKER_DIR/explorer/config"

    chmod 700 "$DOCKER_DIR/explorer/db" 2>/dev/null || true
    chmod 750 "$DOCKER_DIR/explorer/config" 2>/dev/null || true
}

# --- Health Checks ---

# Utility function to wait for container to be ready
wait_for_container_ready() {
    CONTAINER_NAME="$1"
    READY_MESSAGE="$2"
    TIMEOUT="${3:-60}"
    EDITION_TYPE="${4:-}"
    LICENSE_TYPE="${5:-}"

    printf "├─ Starting InfluxDB"
    ELAPSED=0
    EMAIL_MESSAGE_SHOWN=false

    while [ $ELAPSED -lt $TIMEOUT ]; do
        if docker logs "$CONTAINER_NAME" 2>&1 | grep -q "$READY_MESSAGE"; then
            printf "${NC}\n"
            return 0
        fi

        if ! docker ps | grep -q "$CONTAINER_NAME"; then
            printf "${NC}\n"
            printf "├─${RED} Error: InfluxDB container stopped unexpectedly${NC}\n"
            docker logs --tail 20 "$CONTAINER_NAME"
            return 1
        fi

        if [ $ELAPSED -eq $((TIMEOUT - 1)) ]; then
            printf "${NC}\n"
            printf "├─${RED} Error: InfluxDB failed to start within ${TIMEOUT} seconds${NC}\n"
            return 1
        fi

        # Show email verification message after 4 seconds for Enterprise with new license
        if [ "$EDITION_TYPE" = "enterprise" ] && [ -n "$LICENSE_TYPE" ] && \
           [ $ELAPSED -ge 4 ] && [ "$EMAIL_MESSAGE_SHOWN" = "false" ]; then
            printf "${NC}\n"
            printf "├─${YELLOW} License activation requires email verification${NC}\n"
            printf "├─${BOLD} → Check your inbox and verify your email address${NC}\n"
            printf "├─ Continuing startup"
            EMAIL_MESSAGE_SHOWN=true
        fi

        printf "."
        sleep 2
        ELAPSED=$((ELAPSED + 2))
    done
}

# Utility function to wait for Explorer to be fully ready
wait_for_explorer_ready() {
    TIMEOUT="${1:-60}"

    printf "└─ Starting Explorer"
    ELAPSED=0

    while [ $ELAPSED -lt $TIMEOUT ]; do
        if curl -s http://localhost:$EXPLORER_PORT >/dev/null 2>&1; then
            API_RESPONSE=$(curl -s http://localhost:$EXPLORER_PORT/api/health 2>&1)
            if echo "$API_RESPONSE" | grep -q "ok\|status\|healthy" 2>/dev/null; then
                printf "${NC}\n"
                return 0
            fi
        fi

        printf "."
        sleep 2
        ELAPSED=$((ELAPSED + 2))
    done
    printf "${NC}\n"
    return 1
}

# --- Token Management ---

# Utility function to create operator token via API
create_operator_token() {
    CONTAINER_NAME="$1"

    # Try API with retries
    MAX_RETRIES=3
    RETRY=0
    TOKEN=""

    while [ $RETRY -lt $MAX_RETRIES ] && [ -z "$TOKEN" ]; do
        TOKEN_RESPONSE=$(curl -s -w "\n%{http_code}" -m 5 -X POST http://localhost:$INFLUXDB_PORT/api/v3/configure/token/admin 2>&1)

        # Extract HTTP status code (last line)
        HTTP_CODE=$(echo "$TOKEN_RESPONSE" | tail -n1)
        # Extract response body (all but last line)
        RESPONSE_BODY=$(echo "$TOKEN_RESPONSE" | sed '$d')

        # Check for 409 conflict
        if [ "$HTTP_CODE" = "409" ]; then
            echo "TOKEN_ALREADY_EXISTS"
            return 2
        fi

        # Try to extract token from response
        TOKEN=$(echo "$RESPONSE_BODY" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)

        if [ -z "$TOKEN" ] && [ $RETRY -lt $((MAX_RETRIES - 1)) ]; then
            sleep 2
            RETRY=$((RETRY + 1))
        else
            break
        fi
    done

    # Return token or manual creation message
    if [ -n "$TOKEN" ]; then
        echo "$TOKEN"
        return 0
    else
        echo "$MANUAL_TOKEN_MSG"
        return 1
    fi
}

# --- Configuration Generation ---

# Function to generate Docker Compose YAML
generate_docker_compose_yaml() {
    EDITION_TYPE="$1"  # "core" or "enterprise"
    SESSION_SECRET="$2"
    LICENSE_EMAIL="$3"
    DOCKER_DIR="$4"

    USER_UID=$(id -u)
    USER_GID=$(id -g)

    # Determine edition-specific values
    if [ "$EDITION_TYPE" = "enterprise" ]; then
        SERVICE_NAME="influxdb3-enterprise"
        IMAGE_NAME="influxdb:3-enterprise"
        CLUSTER_ARG="      - --cluster-id=cluster0"
        ENV_SECTION="    environment:
      - INFLUXDB3_ENTERPRISE_LICENSE_EMAIL=\${INFLUXDB_EMAIL}"
    else
        SERVICE_NAME="influxdb3-core"
        IMAGE_NAME="influxdb:3-core"
        CLUSTER_ARG=""
        ENV_SECTION=""
    fi

    cat > "$DOCKER_DIR/docker-compose.yml" << COMPOSE_EOF
services:
  ${SERVICE_NAME}:
    image: ${IMAGE_NAME}
    container_name: ${SERVICE_NAME}
    user: "${USER_UID}:${USER_GID}"
    ports:
      - "${INFLUXDB_PORT}:8181"
    command:
      - influxdb3
      - serve
      - --node-id=node0${CLUSTER_ARG:+
${CLUSTER_ARG}}
      - --object-store=file
      - --data-dir=/var/lib/influxdb3/data
      - --plugin-dir=/var/lib/influxdb3/plugins${ENV_SECTION:+
${ENV_SECTION}}
    volumes:
      - ~/.influxdb/data:/var/lib/influxdb3/data
      - ~/.influxdb/plugins:/var/lib/influxdb3/plugins
    restart: unless-stopped
    networks:
      - influxdb-network

  influxdb3-explorer:
    image: ${EXPLORER_IMAGE}
    container_name: influxdb3-explorer
    command: ["--mode=admin"]
    ports:
      - "${EXPLORER_PORT}:80"
    volumes:
      - ./explorer/db:/db:rw
      - ./explorer/config:/app-root/config:ro
    environment:
      - SESSION_SECRET_KEY=\${SESSION_SECRET}
    restart: unless-stopped
    depends_on:
      - ${SERVICE_NAME}
    networks:
      - influxdb-network

networks:
  influxdb-network:
    driver: bridge
COMPOSE_EOF

    # Create .env file
    if [ "$EDITION_TYPE" = "enterprise" ]; then
        cat > "$DOCKER_DIR/.env" << ENV_EOF
INFLUXDB_EMAIL=${LICENSE_EMAIL}
SESSION_SECRET=${SESSION_SECRET}
ENV_EOF
    else
        cat > "$DOCKER_DIR/.env" << ENV_EOF
SESSION_SECRET=${SESSION_SECRET}
ENV_EOF
    fi

    # Set restrictive permissions on .env file
    chmod 600 "$DOCKER_DIR/.env"
}

# Function to configure Explorer via file
configure_explorer_via_file() {
    TOKEN="$1"
    INFLUXDB_URL="$2"
    SERVER_NAME="$3"
    DOCKER_DIR="$4"

    printf "├─ Configuring Explorer...\n"

    # Ensure config directory exists with correct permissions
    mkdir -p "$DOCKER_DIR/explorer/config"
    chmod 755 "$DOCKER_DIR/explorer/config"

    # Create the config.json file
    cat > "$DOCKER_DIR/explorer/config/config.json" <<EOF
{
  "DEFAULT_INFLUX_SERVER": "$INFLUXDB_URL",
  "DEFAULT_API_TOKEN": "$TOKEN",
  "DEFAULT_SERVER_NAME": "$SERVER_NAME"
}
EOF

    chmod 640 "$DOCKER_DIR/explorer/config/config.json"

    return 0
}

# Utility function to extract existing token from Explorer config
extract_token_from_explorer_config() {
    DOCKER_DIR="$1"
    CONFIG_FILE="$DOCKER_DIR/explorer/config/config.json"

    if [ -f "$CONFIG_FILE" ]; then
        TOKEN=$(grep '"DEFAULT_API_TOKEN"' "$CONFIG_FILE" | cut -d'"' -f4)
        if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
            echo "$TOKEN"
            return 0
        fi
    fi
    echo ""
    return 1
}

# ==============================================================================
# SECTION 6: DOCKER COMPOSE ORCHESTRATION
# ==============================================================================
# High-level Docker Compose setup workflow

# Unified function to setup Docker Compose (both Core and Enterprise)
setup_docker_compose() {
    EDITION_TYPE="$1"  # "core" or "enterprise"
    DOCKER_DIR="${2:-$HOME/.influxdb/docker}"
    LICENSE_EMAIL="$3"
    LICENSE_TYPE="$4"

    # Set edition-specific values
    if [ "$EDITION_TYPE" = "enterprise" ]; then
        EDITION_NAME="Enterprise"
        CONTAINER_NAME="influxdb3-enterprise"
        SERVER_NAME="InfluxDB 3 Enterprise"
    else
        EDITION_NAME="Core"
        CONTAINER_NAME="influxdb3-core"
        SERVER_NAME="InfluxDB 3 Core"
    fi

    printf "\n${BOLD}Setting up InfluxDB 3 ${EDITION_NAME}${NC}\n"

    # Verify Docker is running before doing any setup work (silent check)
    if ! check_docker; then
        printf "${RED}Error: Docker is not running${NC}\n\n"
        printf "${BOLD}Docker Connection Failed${NC}\n"
        printf "Docker is not responding. Please ensure Docker Desktop is running.\n\n"
        printf "${BOLD}How to fix:${NC}\n"
        printf "  • Open Docker Desktop and wait for it to start\n"
        printf "  • Check Docker Desktop status in your system tray\n"
        printf "  • Run ${BOLD}docker info${NC} to verify Docker is responding\n"
        printf "  • Restart Docker Desktop if necessary, then run this script again\n\n"
        return 1
    fi

    # Enterprise-specific: Handle license prompting
    if [ "$EDITION_TYPE" = "enterprise" ]; then
        # Check for existing license in shared data directory
        if [ -f "$HOME/.influxdb/data/cluster0/trial_or_home_license" ]; then
            printf "├─${DIM} Found existing license file${NC}\n"
        elif [ -z "$LICENSE_EMAIL" ]; then
            # Prompt for license if not provided
            printf "\n${BOLD}License Setup Required${NC}\n"
            printf "1) ${GREEN}Trial${NC} ${DIM}- Full features for 30 days (up to 256 cores)${NC}\n"
            printf "2) ${GREEN}Home${NC} ${DIM}- Free for non-commercial use (max 2 cores, single node)${NC}\n"
            printf "\nEnter your choice (1-2): "
            read -r LICENSE_CHOICE

            case "${LICENSE_CHOICE:-1}" in
                1) LICENSE_TYPE="trial" ;;
                2) LICENSE_TYPE="home" ;;
                *) LICENSE_TYPE="trial" ;;
            esac

            printf "Enter your email: "
            read -r LICENSE_EMAIL
            while [ -z "$LICENSE_EMAIL" ]; do
                printf "Email is required. Enter your email: "
                read -r LICENSE_EMAIL
            done
        fi
    fi

    printf "├─ Creating directories\n"
    create_docker_directories "$DOCKER_DIR"

    # Stop any existing containers before reconfiguring
    if [ -f "$DOCKER_DIR/docker-compose.yml" ]; then
        printf "├─ Stopping existing containers\n"
        cd "$DOCKER_DIR"
        docker compose down 2>/dev/null || true
        cd - > /dev/null
    fi

    # Check for available ports
    INFLUXDB_PORT=$(find_next_available_port "$INFLUXDB_PORT")
    EXPLORER_PORT=$(find_next_available_port "$EXPLORER_PORT")

    # Generate session secret
    SESSION_SECRET=$(generate_session_secret)

    printf "├─ Creating docker-compose.yml\n"
    generate_docker_compose_yaml "$EDITION_TYPE" "$SESSION_SECRET" "$LICENSE_EMAIL" "$DOCKER_DIR"

    cd "$DOCKER_DIR"

    # Pull InfluxDB image
    if ! pull_docker_image "$CONTAINER_NAME" "InfluxDB 3 ${EDITION_NAME}" "true"; then
        return 1
    fi

    docker compose up -d "$CONTAINER_NAME" 2>&1 | filter_docker_output

    # Wait for InfluxDB to be ready
    if ! wait_for_container_ready "$CONTAINER_NAME" "startup time:" 60 "$EDITION_TYPE" "$LICENSE_TYPE"; then
        return 1
    fi

    # Check for existing token in Explorer config, or create new one
    CONFIG_FILE="$DOCKER_DIR/explorer/config/config.json"
    TOKEN_IS_NEW=false
    EXPLORER_DEPLOYED=false

    if [ -f "$CONFIG_FILE" ]; then
        printf "└─ Found existing Explorer config\n\n"
        TOKEN=$(extract_token_from_explorer_config "$DOCKER_DIR")

        if [ -n "$TOKEN" ]; then
            TOKEN_IS_NEW=false
        else
            printf "   ${YELLOW}Config exists but no valid token found${NC}\n"
            printf "   Creating new operator token\n"
            TOKEN=$(create_operator_token "$CONTAINER_NAME")
            TOKEN_IS_NEW=true
            printf "\n"
        fi
    else
        printf "└─ Creating operator token\n"
        TOKEN=$(create_operator_token "$CONTAINER_NAME")
        TOKEN_IS_NEW=true
        printf "\n"
    fi

    # Display token BEFORE launching Explorer (only if newly created)
    if [ "$TOKEN" != "$MANUAL_TOKEN_MSG" ] && [ "$TOKEN" != "TOKEN_ALREADY_EXISTS" ]; then
        if [ "$TOKEN_IS_NEW" = true ]; then
            printf "${BOLD}════════════════════════════════════════════════════════════${NC}\n"
            printf "${BOLD}OPERATOR TOKEN: Save this token. It will not be shown again.${NC}\n"
            printf "%s\n" "$TOKEN"
            printf "${BOLD}════════════════════════════════════════════════════════════${NC}\n\n"
        fi

        # Now launch Explorer after showing the token
        printf "${BOLD}Setting up InfluxDB 3 Explorer...${NC}\n"

        # Pull Explorer image
        pull_docker_image "influxdb3-explorer" "InfluxDB 3 Explorer" "false"

        # Configure Explorer (use port 8181 for container-to-container communication)
        configure_explorer_via_file "$TOKEN" "http://${CONTAINER_NAME}:8181" "$SERVER_NAME" "$DOCKER_DIR"
        docker compose up -d influxdb3-explorer 2>&1 | filter_docker_output

        # Wait for Explorer to be ready
        if wait_for_explorer_ready 60; then
            EXPLORER_DEPLOYED=true
            open_browser_url "http://localhost:${EXPLORER_PORT}/system-overview"
        fi
    else
        # Token creation failed - prepare Explorer but don't start it
        printf "${BOLD}Setting up InfluxDB 3 Explorer...${NC}\n"

        # Pull Explorer image
        pull_docker_image "influxdb3-explorer" "InfluxDB 3 Explorer" "false"

        # Configure Explorer with placeholder token
        configure_explorer_via_file "YOUR_TOKEN_HERE" "http://${CONTAINER_NAME}:8181" "$SERVER_NAME" "$DOCKER_DIR"

        printf "└─ Explorer prepared (manual token configuration required)\n\n"
    fi

    # Display success message and access points AFTER Explorer launch
    if [ "$EXPLORER_DEPLOYED" = true ]; then
        printf "\n${BOLDGREEN}✓ InfluxDB 3 ${EDITION_NAME} with InfluxDB 3 Explorer successfully deployed${NC}\n\n"
    else
        printf "\n${BOLDGREEN}✓ InfluxDB 3 ${EDITION_NAME} successfully deployed${NC}\n\n"
    fi
    printf "${BOLD}Access Points:${NC}\n"
    if [ "$EXPLORER_DEPLOYED" = true ]; then
        printf "├─ Explorer UI:  ${BLUE}http://localhost:${EXPLORER_PORT}${NC}\n"
    fi
    printf "└─ InfluxDB API: ${BLUE}http://localhost:${INFLUXDB_PORT}${NC}\n\n"

    printf "${BOLD}Storage Locations:${NC}\n"
    printf "├─ InfluxDB Data:    ${DIM}%s/.influxdb/data${NC}\n" "$HOME"
    if [ "$EXPLORER_DEPLOYED" = true ]; then
        printf "├─ InfluxDB Plugins: ${DIM}%s/.influxdb/plugins${NC}\n" "$HOME"
        printf "└─ Explorer DB:      ${DIM}%s/explorer/db${NC}\n\n" "$DOCKER_DIR"
    else
        printf "└─ InfluxDB Plugins: ${DIM}%s/.influxdb/plugins${NC}\n\n" "$HOME"
    fi

    printf "${BOLD}Configuration Files:${NC}\n"
    printf "├─ Docker Compose:   ${DIM}%s/docker-compose.yml${NC}\n" "$DOCKER_DIR"
    printf "└─ Explorer Config:  ${DIM}%s/explorer/config/config.json${NC}\n\n" "$DOCKER_DIR"

    # Show manual setup instructions if Explorer was not deployed
    if [ "$EXPLORER_DEPLOYED" = false ]; then
        printf "${BOLD}To Complete InfluxDB 3 Explorer Setup:${NC}\n"
        printf "1. Replace ${DIM}\"YOUR_TOKEN_HERE\"${NC} with your admin token in the Explorer config file:\n"
        printf "   ${DIM}%s/explorer/config/config.json${NC}\n\n" "$DOCKER_DIR"
        printf "2. Start InfluxDB 3 Explorer:\n"
        printf "   ${DIM}cd %s && docker compose up -d influxdb3-explorer${NC}\n\n" "$DOCKER_DIR"
        printf "3. Open your browser to http://localhost:${EXPLORER_PORT}\n\n"
    fi

    return 0
}

# ==============================================================================
# SECTION 7: BINARY INSTALLATION UTILITIES
# ==============================================================================
# Low-level binary installation helpers

# --- Quick Start Configuration ---

# Function to set up Quick Start defaults for both Core and Enterprise
setup_quick_start_defaults() {
    edition="${1:-core}"

    NODE_ID="node0"
    STORAGE_TYPE="File Storage"
    STORAGE_PATH="$HOME/.influxdb/data"
    PLUGIN_PATH="$HOME/.influxdb/plugins"
    STORAGE_FLAGS="--object-store=file --data-dir ${STORAGE_PATH} --plugin-dir ${PLUGIN_PATH}"
    STORAGE_FLAGS_ECHO="--object-store=file --data-dir ${STORAGE_PATH} --plugin-dir ${PLUGIN_PATH}"
    START_SERVICE="y"  # Always set for Quick Start

    # Enterprise-specific settings
    if [ "$edition" = "enterprise" ]; then
        CLUSTER_ID="cluster0"
        LICENSE_FILE_PATH="${STORAGE_PATH}/${CLUSTER_ID}/trial_or_home_license"
    fi

    # Create directories
    mkdir -p "${STORAGE_PATH}"
    mkdir -p "${PLUGIN_PATH}"
}

# --- Cloud Storage Configuration ---

# Function to configure AWS S3 storage
configure_aws_s3_storage() {
    printf "\n"
    printf "${BOLD}AWS S3 Configuration${NC}\n"
    printf "├─ Enter AWS Access Key ID: "
    read -r AWS_KEY

    printf "├─ Enter AWS Secret Access Key: "
    stty -echo
    read -r AWS_SECRET
    stty echo

    printf "\n"
    printf "├─ Enter S3 Bucket: "
    read -r AWS_BUCKET

    printf "└─ Enter AWS Region (default: us-east-1): "
    read -r AWS_REGION
    AWS_REGION=${AWS_REGION:-"us-east-1"}

    STORAGE_FLAGS="--object-store=s3 --bucket=${AWS_BUCKET}"
    if [ -n "$AWS_REGION" ]; then
        STORAGE_FLAGS="$STORAGE_FLAGS --aws-default-region=${AWS_REGION}"
    fi
    STORAGE_FLAGS="$STORAGE_FLAGS --aws-access-key-id=${AWS_KEY}"
    STORAGE_FLAGS_ECHO="$STORAGE_FLAGS --aws-secret-access-key=..."
    STORAGE_FLAGS="$STORAGE_FLAGS --aws-secret-access-key=${AWS_SECRET}"
}

# Function to configure Azure storage
configure_azure_storage() {
    printf "\n"
    printf "${BOLD}Azure Storage Configuration${NC}\n"
    printf "├─ Enter Storage Account Name: "
    read -r AZURE_ACCOUNT

    printf "└─ Enter Storage Access Key: "
    stty -echo
    read -r AZURE_KEY
    stty echo

    printf "\n"
    STORAGE_FLAGS="--object-store=azure --azure-storage-account=${AZURE_ACCOUNT}"
    STORAGE_FLAGS_ECHO="$STORAGE_FLAGS --azure-storage-access-key=..."
    STORAGE_FLAGS="$STORAGE_FLAGS --azure-storage-access-key=${AZURE_KEY}"
}

# Function to configure Google Cloud storage
configure_google_cloud_storage() {
    printf "\n"
    printf "${BOLD}Google Cloud Storage Configuration${NC}\n"
    printf "└─ Enter path to service account JSON file: "
    read -r GOOGLE_SA
    STORAGE_FLAGS="--object-store=google --google-service-account=${GOOGLE_SA}"
    STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
}

# --- Enterprise License ---

# Function to set up license for Enterprise Quick Start
setup_license_for_quick_start() {
    # Check if license file exists
    if [ -f "$LICENSE_FILE_PATH" ]; then
        printf "${DIM}Found existing license file, using it for quick start.${NC}\n"
        LICENSE_TYPE=""
        LICENSE_EMAIL=""
        LICENSE_DESC="Existing"
    else
        # Prompt for license type and email only
        printf "\n"
        printf "${BOLD}License Setup Required${NC}\n"
        printf "1) ${GREEN}Trial${NC} ${DIM}- Full features for 30 days (up to 256 cores)${NC}\n"
        printf "2) ${GREEN}Home${NC} ${DIM}- Free for non-commercial use (max 2 cores, single node)${NC}\n"
        printf "\n"
        printf "Enter your choice (1-2): "
        read -r LICENSE_CHOICE

        case "${LICENSE_CHOICE:-1}" in
            1)
                LICENSE_TYPE="trial"
                LICENSE_DESC="Trial"
                ;;
            2)
                LICENSE_TYPE="home"
                LICENSE_DESC="Home"
                ;;
            *)
                LICENSE_TYPE="trial"
                LICENSE_DESC="Trial"
                ;;
        esac

        printf "Enter your email: "
        read -r LICENSE_EMAIL
        while [ -z "$LICENSE_EMAIL" ]; do
            printf "Email is required. Enter your email: "
            read -r LICENSE_EMAIL
        done
    fi
}

# --- Storage Configuration Prompts ---

# Function to prompt for storage configuration
prompt_storage_configuration() {
    # Prompt for storage solution
    printf "\n"
    printf "${BOLD}Select Your Storage Solution${NC}\n"
    printf "├─ 1) File storage (Persistent)\n"
    printf "├─ 2) Object storage (Persistent)\n"
    printf "├─ 3) In-memory storage (Non-persistent)\n"
    printf "└─ Enter your choice (1-3): "
    read -r STORAGE_CHOICE

    case "$STORAGE_CHOICE" in
        1)
            STORAGE_TYPE="File Storage"
            printf "\n"
            printf "Enter storage path (default: %s/data): " "${INSTALL_LOC}"
            read -r STORAGE_PATH
            STORAGE_PATH=${STORAGE_PATH:-"$INSTALL_LOC/data"}
            STORAGE_FLAGS="--object-store=file --data-dir ${STORAGE_PATH}"
            STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
            ;;
        2)
            STORAGE_TYPE="Object Storage"
            printf "\n"
            printf "${BOLD}Select Cloud Provider${NC}\n"
            printf "├─ 1) Amazon S3\n"
            printf "├─ 2) Azure Storage\n"
            printf "├─ 3) Google Cloud Storage\n"
            printf "└─ Enter your choice (1-3): "
            read -r CLOUD_CHOICE

            case $CLOUD_CHOICE in
                1)  # AWS S3
                    configure_aws_s3_storage
                    ;;

                2)  # Azure Storage
                    configure_azure_storage
                    ;;

                3)  # Google Cloud Storage
                    configure_google_cloud_storage
                    ;;

                *)
                    printf "Invalid cloud provider choice. Defaulting to file storage.\n"
                    STORAGE_TYPE="File Storage"
                    STORAGE_FLAGS="--object-store=file --data-dir $INSTALL_LOC/data"
                    STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
                    ;;
            esac
            ;;
        3)
            STORAGE_TYPE="memory"
            STORAGE_FLAGS="--object-store=memory"
            STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
            ;;

        *)
            printf "Invalid choice. Defaulting to file storage.\n"
            STORAGE_TYPE="File Storage"
            STORAGE_FLAGS="--object-store=file --data-dir $INSTALL_LOC/data"
            STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
            ;;
    esac
}

# --- Server Management ---

# Function to perform health check on server
perform_server_health_check() {
    timeout_seconds="${1:-30}"
    is_enterprise="${2:-false}"

    SUCCESS=0
    EMAIL_MESSAGE_SHOWN=false

    for i in $(seq 1 "$timeout_seconds"); do
        # on systems without a usable lsof, sleep a second to see if the pid is
        # still there to give influxdb a chance to error out in case an already
        # running influxdb is running on this port
        if [ -z "$lsof_exec" ]; then
            sleep 1
        fi

        if ! kill -0 "$PID" 2>/dev/null ; then
            if [ "$is_enterprise" = "true" ]; then
                printf "└─${DIM} Server process stopped unexpectedly${NC}\n"
            fi
            break
        fi

        if curl --max-time 1 -s "http://localhost:$PORT/health" >/dev/null 2>&1; then
            printf "\n${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed and running on port %s. Nice!${NC}\n" "$PORT"
            SUCCESS=1
            break
        fi

        # Show email verification message after 10 seconds for Enterprise
        if [ "$is_enterprise" = "true" ] && [ "$i" -eq 10 ] && [ "$EMAIL_MESSAGE_SHOWN" = "false" ]; then
            printf "├─${YELLOW} License activation requires email verification${NC}\n"
            printf "├─${BOLD} → Check your inbox and click the verification link${NC}\n"
            EMAIL_MESSAGE_SHOWN=true
        fi

        # Show progress updates every 15 seconds after initial grace period
        if [ "$is_enterprise" = "true" ] && [ "$i" -gt 5 ] && [ $((i % 15)) -eq 0 ]; then
            printf "├─${DIM} Waiting for license verification (%s/%ss)${NC}\n" "$i" "$timeout_seconds"
        fi

        sleep 1
    done

    if [ $SUCCESS -eq 0 ]; then
        if [ "$is_enterprise" = "true" ]; then
            printf "├─${BOLD} Error: InfluxDB Enterprise failed to start within %s seconds${NC}\n\n" "$timeout_seconds"
            if [ -n "$LICENSE_TYPE" ]; then
                printf "   ├─${YELLOW} Check your email for license verification${NC} ${BOLD}(REQUIRED)${NC}\n"
                printf "   │  ${BOLD}→ Click the verification link in your inbox${NC}\n"
            fi
            printf "   ├─ Check for network connectivity issues\n"
            printf "   └─ Check for port %s conflicts\n" "$PORT"

            # Kill the background process if it's still running
            if kill -0 "$PID" 2>/dev/null; then
                printf "   Stopping background server process...\n"
                kill "$PID" 2>/dev/null
            fi
        else
            printf "└─${BOLD} Error: InfluxDB failed to start; check permissions or other potential issues.${NC}\n"
            exit 1
        fi
    fi
}

# Function to display Enterprise server command
display_enterprise_server_command() {
    is_quick_start="${1:-false}"

    if [ "$is_quick_start" = "true" ]; then
        # Quick Start format
        printf "└─${DIM} Command: ${NC}\n"
        printf "${DIM}   influxdb3 serve \\\\${NC}\n"
        printf "${DIM}   --cluster-id=%s \\\\${NC}\n" "$CLUSTER_ID"
        printf "${DIM}   --node-id=%s \\\\${NC}\n" "$NODE_ID"
        if [ -n "$LICENSE_TYPE" ] && [ -n "$LICENSE_EMAIL" ]; then
            printf "${DIM}   --license-type=%s \\\\${NC}\n" "$LICENSE_TYPE"
            printf "${DIM}   --license-email=%s \\\\${NC}\n" "$LICENSE_EMAIL"
        fi
        printf "${DIM}   --http-bind=0.0.0.0:%s \\\\${NC}\n" "$PORT"
        printf "${DIM}   %s${NC}\n" "$STORAGE_FLAGS_ECHO"
        printf "\n"
    else
        # Custom configuration format
        printf "│\n"
        printf "├─ Running serve command:\n"
        printf "├─${DIM} influxdb3 serve \\\\${NC}\n"
        printf "├─${DIM} --cluster-id='%s' \\\\${NC}\n" "$CLUSTER_ID"
        printf "├─${DIM} --node-id='%s' \\\\${NC}\n" "$NODE_ID"
        printf "├─${DIM} --license-type='%s' \\\\${NC}\n" "$LICENSE_TYPE"
        printf "├─${DIM} --license-email='%s' \\\\${NC}\n" "$LICENSE_EMAIL"
        printf "├─${DIM} --http-bind='0.0.0.0:%s' \\\\${NC}\n" "$PORT"
        printf "├─${DIM} %s${NC}\n" "$STORAGE_FLAGS_ECHO"
        printf "│\n"
    fi
}

# ==============================================================================
# SECTION 8: MAIN INSTALLATION FLOW
# ==============================================================================

# --- Phase 1: Welcome and Pre-Checks ---

# Attempt to clear screen and show welcome message
clear 2>/dev/null || true  # clear isn't available everywhere
printf "┌───────────────────────────────────────────────────┐\n"
printf "│ ${BOLD}Welcome to InfluxDB!${NC} We'll make this quick.       │\n"
printf "└───────────────────────────────────────────────────┘\n"

# Check for edition mismatch
CURRENT_EDITION=""
if [ -f "$HOME/.influxdb/docker/docker-compose.yml" ]; then
    if grep -q "influxdb3-enterprise:" "$HOME/.influxdb/docker/docker-compose.yml" 2>/dev/null; then
        CURRENT_EDITION="Enterprise"
    elif grep -q "influxdb3-core:" "$HOME/.influxdb/docker/docker-compose.yml" 2>/dev/null; then
        CURRENT_EDITION="Core"
    fi
elif [ -f "$HOME/.influxdb/influxdb3" ]; then
    # Binary exists - try to detect edition from binary if possible
    # For now, we'll need the user to have run it at least once
    if [ -d "$HOME/.influxdb/data" ]; then
        # Check for Enterprise-specific license file
        if find "$HOME/.influxdb/data" -name "trial_or_home_license" 2>/dev/null | grep -q .; then
            CURRENT_EDITION="Enterprise"
        else
            CURRENT_EDITION="Core"
        fi
    fi
fi

# Display notice if trying to install different edition
if [ -n "$CURRENT_EDITION" ] && [ "$CURRENT_EDITION" != "$EDITION" ]; then
    printf "\n${YELLOW}Notice:${NC} You have InfluxDB 3 ${BOLD}%s${NC} currently installed, but are installing InfluxDB 3 ${BOLD}%s${NC}.\n" "$CURRENT_EDITION" "$EDITION"
    if [ "$EDITION" = "Enterprise" ]; then
        printf "To install %s, remove 'enterprise' as an argument to your script execution command.\n" "$CURRENT_EDITION"
    else
        printf "To install %s, add 'enterprise' as an argument to your script execution command.\n" "$CURRENT_EDITION"
    fi
fi

# --- Phase 2: Installation Type Selection ---

printf "\n"
printf "${BOLD}Select Installation Type${NC}\n"
printf "\n"
printf "1) ${GREEN}Docker Compose${NC}  ${DIM}(Installs InfluxDB 3 %s + Explorer UI)${NC}\n" "$EDITION"
printf "2) ${GREEN}Simple Download${NC} ${DIM}(Binary installation, no dependencies)${NC}\n"
printf "\n"
printf "Enter your choice (1-2): "
read -r INSTALL_TYPE

case "$INSTALL_TYPE" in
    1)
        # Docker Compose installation
        if ! check_docker; then
            printf "\n${RED}Error:${NC} Docker is not installed or not running, or you are not in the Docker user group.\n"
            printf "Please install Docker Desktop or Docker Engine with the compose plugin and try again.\n"
            printf "Visit: ${BLUE}https://www.docker.com/${NC}\n\n"
            exit 1
        fi

        # Set default installation directory
        DOCKER_DIR="$HOME/.influxdb/docker"

        # Run appropriate setup based on edition
        if [ "$EDITION" = "Enterprise" ]; then
            setup_docker_compose "enterprise" "$DOCKER_DIR"
        else
            setup_docker_compose "core" "$DOCKER_DIR"
        fi

        exit 0
        ;;
    2)
        # Binary installation selected - proceed directly with download
        printf "\n\n"
        ;;
    *)
        printf "Invalid choice. Defaulting to binary installation.\n\n"
        ;;
esac

# attempt to find the user's shell config
shellrc=
if [ -n "$SHELL" ]; then
    tmp=~/.$(basename "$SHELL")rc
    if [ -e "$tmp" ]; then
        shellrc="$tmp"
    fi
fi

printf "${BOLD}Downloading InfluxDB 3 %s to %s${NC}\n" "$EDITION" "$INSTALL_LOC"
printf "├─${DIM} mkdir -p '%s'${NC}\n" "$INSTALL_LOC"
mkdir -p "$INSTALL_LOC"
printf "└─${DIM} curl -sSL '%s' -o '%s/influxdb3-${EDITION_TAG}.tar.gz'${NC}\n" "${URL}" "$INSTALL_LOC"
curl -sSL "${URL}" -o "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz"

printf "\n"
printf "${BOLD}Verifying '%s/influxdb3-${EDITION_TAG}.tar.gz'${NC}\n" "$INSTALL_LOC"
printf "└─${DIM} curl -sSL '%s.sha256' -o '%s/influxdb3-${EDITION_TAG}.tar.gz.sha256'${NC}\n" "${URL}" "$INSTALL_LOC"
curl -sSL "${URL}.sha256" -o "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz.sha256"
dl_sha=$(cut -d ' ' -f 1 "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz.sha256" | grep -E '^[0-9a-f]{64}$')
if [ -z "$dl_sha" ]; then
    printf "Could not find properly formatted SHA256 in '%s/influxdb3-${EDITION_TAG}.tar.gz.sha256'. Aborting.\n" "$INSTALL_LOC"
    exit 1
fi

ch_sha=
if [ "${OS}" = "Darwin" ]; then
    printf "└─${DIM} shasum -a 256 '%s/influxdb3-${EDITION_TAG}.tar.gz'" "$INSTALL_LOC"
    ch_sha=$(shasum -a 256 "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz" | cut -d ' ' -f 1)
else
    printf "└─${DIM} sha256sum '%s/influxdb3-${EDITION_TAG}.tar.gz'" "$INSTALL_LOC"
    ch_sha=$(sha256sum "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz" | cut -d ' ' -f 1)
fi
if [ "$ch_sha" = "$dl_sha" ]; then
    printf " (OK: %s = %s)${NC}\n" "$ch_sha" "$dl_sha"
else
    printf " (Error: %s != %s). Aborting.${NC}\n" "$ch_sha" "$dl_sha"
    exit 1
fi
printf "└─${DIM} rm '%s/influxdb3-${EDITION_TAG}.tar.gz.sha256'${NC}\n" "$INSTALL_LOC"
rm "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz.sha256"

printf "\n"
printf "${BOLD}Extracting and Processing${NC}\n"

# some tarballs have a leading component, check for that
TAR_LEVEL=0
if tar -tf "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz" | grep -q '[a-zA-Z0-9]/influxdb3$' ; then
    TAR_LEVEL=1
fi
printf "├─${DIM} tar -xf '%s/influxdb3-${EDITION_TAG}.tar.gz' --strip-components=${TAR_LEVEL} -C '%s'${NC}\n" "$INSTALL_LOC" "$INSTALL_LOC"
tar -xf "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz" --strip-components="${TAR_LEVEL}" -C "$INSTALL_LOC"

printf "└─${DIM} rm '%s/influxdb3-${EDITION_TAG}.tar.gz'${NC}\n" "$INSTALL_LOC"
rm "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz"

if [ -n "$shellrc" ] && ! grep -q "export PATH=.*$INSTALL_LOC" "$shellrc"; then
    printf "\n"
    printf "${BOLD}Adding InfluxDB to '%s'${NC}\n" "$shellrc"
    printf "└─${DIM} export PATH=\"\$PATH:%s/\" >> '%s'${NC}\n" "$INSTALL_LOC" "$shellrc"
    echo "export PATH=\"\$PATH:$INSTALL_LOC/\"" >> "$shellrc"
fi

export INFLUXDB3_SERVE_INVOCATION_METHOD="install-script"

if [ "${EDITION}" = "Core" ]; then
    # Prompt user for startup options
    printf "\n"
    printf "${BOLD}What would you like to do next?${NC}\n"
    printf "1) ${GREEN}Quick Start${NC} ${DIM}(recommended; data stored at %s/data)${NC}\n" "${INSTALL_LOC}"
    printf "2) ${GREEN}Custom Configuration${NC} ${DIM}(configure all options manually)${NC}\n"
    printf "3) ${GREEN}Skip startup${NC} ${DIM}(install only)${NC}\n"
    printf "\n"
    printf "Enter your choice (1-3): "
    read -r STARTUP_CHOICE
    STARTUP_CHOICE=${STARTUP_CHOICE:-1}

    case "$STARTUP_CHOICE" in
        1)
            # Quick Start - use defaults
            setup_quick_start_defaults core
            ;;
        2)
            # Custom Configuration - existing detailed flow
            START_SERVICE="y"
            ;;
        3)
            # Skip startup
            START_SERVICE="n"
            ;;
        *)
            printf "Invalid choice. Using Quick Start (option 1).\n"
            setup_quick_start_defaults core
            ;;
    esac

    if [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "2" ]; then
        # Prompt for Node ID
        printf "\n"
        printf "${BOLD}Enter Your Node ID${NC}\n"
        printf "├─ A Node ID is a unique, uneditable identifier for a service.\n"
        printf "└─ Enter a Node ID (default: node0): "
        read -r NODE_ID
        NODE_ID=${NODE_ID:-node0}

        # Prompt for storage solution
        prompt_storage_configuration

        # Ensure port is available; if not, find a new one.
        PORT=$(find_next_available_port "$PORT")

        # Start and give up to 30 seconds to respond
        printf "\n"

        # Create logs directory and generate timestamped log filename
        mkdir -p "$INSTALL_LOC/logs"
        LOG_FILE="$INSTALL_LOC/logs/$(date +%Y%m%d_%H%M%S).log"

        printf "${BOLD}Starting InfluxDB${NC}\n"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} Storage: %s${NC}\n" "$STORAGE_TYPE"
        printf "├─${DIM} Logs: %s${NC}\n" "$LOG_FILE"
        printf "├─${DIM} influxdb3 serve \\\\${NC}\n"
        printf "├─${DIM}   --node-id='%s' \\\\${NC}\n" "$NODE_ID"
        printf "├─${DIM}   --http-bind='0.0.0.0:%s' \\\\${NC}\n" "$PORT"
        printf "└─${DIM}   %s${NC}\n" "$STORAGE_FLAGS_ECHO"

        "$INSTALL_LOC/$BINARY_NAME" serve --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS >> "$LOG_FILE" 2>&1 &
        PID="$!"

        perform_server_health_check 30

    elif [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "1" ]; then
        # Quick Start flow - minimal output, just start the server
        printf "\n"
        printf "${BOLD}Starting InfluxDB (Quick Start)${NC}\n"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} Storage: %s/data${NC}\n" "${INSTALL_LOC}"
        printf "├─${DIM} Plugins: %s/plugins${NC}\n" "${INSTALL_LOC}"
        printf "├─${DIM} Logs: %s/logs/$(date +%Y%m%d_%H%M%S).log${NC}\n" "${INSTALL_LOC}"

        # Ensure port is available; if not, find a new one.
        ORIGINAL_PORT="$PORT"
        PORT=$(find_next_available_port "$PORT")

        # Show port result
        if [ "$PORT" != "$ORIGINAL_PORT" ]; then
            printf "├─${DIM} Found available port: %s (%s-%s in use)${NC}\n" "$PORT" "$ORIGINAL_PORT" "$((PORT - 1))"
        fi

        # Show the command being executed
        printf "└─${DIM} Command:${NC}\n"
        printf "${DIM}    influxdb3 serve \\\\${NC}\n"
        printf "${DIM}     --node-id=%s \\\\${NC}\n" "$NODE_ID"
        printf "${DIM}     --http-bind=0.0.0.0:%s \\\\${NC}\n" "$PORT"
        printf "${DIM}     %s${NC}\n\n" "$STORAGE_FLAGS_ECHO"

        # Create logs directory and generate timestamped log filename
        mkdir -p "$INSTALL_LOC/logs"
        LOG_FILE="$INSTALL_LOC/logs/$(date +%Y%m%d_%H%M%S).log"

        # Start server in background
        "$INSTALL_LOC/$BINARY_NAME" serve --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS >> "$LOG_FILE" 2>&1 &
        PID="$!"

        perform_server_health_check 30

    else
        printf "\n"
        printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed. Nice!${NC}\n"
    fi
else
    # Enterprise startup options
    printf "\n"
    printf "${BOLD}What would you like to do next?${NC}\n"
    printf "1) ${GREEN}Quick Start${NC} ${DIM}(recommended; data stored at %s/data)${NC}\n" "${INSTALL_LOC}"
    printf "2) ${GREEN}Custom Configuration${NC} ${DIM}(configure all options manually)${NC}\n"
    printf "3) ${GREEN}Skip startup${NC} ${DIM}(install only)${NC}\n"
    printf "\n"
    printf "Enter your choice (1-3): "
    read -r STARTUP_CHOICE
    STARTUP_CHOICE=${STARTUP_CHOICE:-1}

    case "$STARTUP_CHOICE" in
        1|*)
            # Quick Start - use defaults and check for existing license
            if [ "$STARTUP_CHOICE" != "1" ]; then
                printf "Invalid choice. Using Quick Start (option 1).\n"
            fi
            setup_quick_start_defaults enterprise
            setup_license_for_quick_start
            START_SERVICE="y"
            ;;
        2)
            # Custom Configuration - existing detailed flow
            START_SERVICE="y"
            ;;
        3)
            # Skip startup
            START_SERVICE="n"
            ;;
    esac

    if [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "1" ]; then
        # Enterprise Quick Start flow
        printf "\n"
        printf "${BOLD}Starting InfluxDB Enterprise (Quick Start)${NC}\n"
        printf "├─${DIM} Cluster ID: %s${NC}\n" "$CLUSTER_ID"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        if [ -n "$LICENSE_TYPE" ]; then
            printf "├─${DIM} License Type: %s${NC}\n" "$LICENSE_DESC"
        fi
        if [ -n "$LICENSE_EMAIL" ]; then
            printf "├─${DIM} Email: %s${NC}\n" "$LICENSE_EMAIL"
        fi
        printf "├─${DIM} Storage: %s/data${NC}\n" "${INSTALL_LOC}"
        printf "├─${DIM} Plugins: %s/plugins${NC}\n" "${INSTALL_LOC}"

        # Create logs directory and generate timestamped log filename
        mkdir -p "$INSTALL_LOC/logs"
        LOG_FILE="$INSTALL_LOC/logs/$(date +%Y%m%d_%H%M%S).log"
        printf "├─${DIM} Logs: %s${NC}\n" "$LOG_FILE"

        # Ensure port is available; if not, find a new one.
        ORIGINAL_PORT="$PORT"
        PORT=$(find_next_available_port "$PORT")

        # Show port result
        if [ "$PORT" != "$ORIGINAL_PORT" ]; then
            printf "├─${DIM} Found available port: %s (%s-%s in use)${NC}\n" "$PORT" "$ORIGINAL_PORT" "$((PORT - 1))"
        fi

        # Show the command being executed
        display_enterprise_server_command true

        # Start server in background with or without license flags
        if [ -n "$LICENSE_TYPE" ] && [ -n "$LICENSE_EMAIL" ]; then
            # New license needed
            "$INSTALL_LOC/$BINARY_NAME" serve --cluster-id="$CLUSTER_ID" --node-id="$NODE_ID" --license-type="$LICENSE_TYPE" --license-email="$LICENSE_EMAIL" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS >> "$LOG_FILE" 2>&1 &
        else
            # Existing license file
            "$INSTALL_LOC/$BINARY_NAME" serve --cluster-id="$CLUSTER_ID" --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS >> "$LOG_FILE" 2>&1 &
        fi
        PID="$!"

        printf "├─${DIM} Server started in background (PID: %s)${NC}\n" "$PID"

        perform_server_health_check 90 true

    elif [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "2" ]; then
        # Enterprise Custom Start flow
        printf "\n"
        # Prompt for Cluster ID
        printf "${BOLD}Enter Your Cluster ID${NC}\n"
        printf "├─ A Cluster ID determines part of the storage path hierarchy.\n"
        printf "├─ All nodes within the same cluster share this identifier.\n"
        printf "└─ Enter a Cluster ID (default: cluster0): "
        read -r CLUSTER_ID
        CLUSTER_ID=${CLUSTER_ID:-cluster0}

        # Prompt for Node ID
        printf "\n"
        printf "${BOLD}Enter Your Node ID${NC}\n"
        printf "├─ A Node ID distinguishes individual server instances within the cluster.\n"
        printf "└─ Enter a Node ID (default: node0): "
        read -r NODE_ID
        NODE_ID=${NODE_ID:-node0}

        # Prompt for license type
        printf "\n"
        printf "${BOLD}Select Your License Type${NC}\n"
        printf "├─ 1) Trial - Full features for 30 days (up to 256 cores)\n"
        printf "├─ 2) Home - Free for non-commercial use (max 2 cores, single node)\n"
        printf "└─ Enter your choice (1-2): "
        read -r LICENSE_CHOICE

        case "$LICENSE_CHOICE" in
            1)
                LICENSE_TYPE="trial"
                LICENSE_DESC="Trial"
                ;;
            2)
                LICENSE_TYPE="home"
                LICENSE_DESC="Home"
                ;;
            *)
                printf "Invalid choice. Defaulting to trial.\n"
                LICENSE_TYPE="trial"
                LICENSE_DESC="Trial"
                ;;
        esac

        # Prompt for email
        printf "\n"
        printf "${BOLD}Enter Your Email Address${NC}\n"
        printf "├─ Required for license verification and activation\n"
        printf "├─${YELLOW} IMPORTANT: You MUST verify your email to activate the license${NC}\n"
        printf "├─${BOLD} → Check your inbox after entering your email${NC}\n"
        printf "└─ Email: "
        read -r LICENSE_EMAIL

        while [ -z "$LICENSE_EMAIL" ]; do
            printf "├─ Email address is required. Please enter your email: "
            read -r LICENSE_EMAIL
        done

        # Prompt for storage solution
        prompt_storage_configuration

        # Ensure port is available; if not, find a new one.
        PORT=$(find_next_available_port "$PORT")

        # Start Enterprise in background with licensing and give up to 90 seconds to respond (licensing takes longer)
        printf "\n"
        printf "${BOLD}Starting InfluxDB Enterprise${NC}\n"
        printf "├─${DIM} Cluster ID: %s${NC}\n" "$CLUSTER_ID"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} License Type: %s${NC}\n" "$LICENSE_DESC"
        printf "├─${DIM} Email: %s${NC}\n" "$LICENSE_EMAIL"
        printf "├─${DIM} Storage: %s${NC}\n" "$STORAGE_TYPE"

        # Create logs directory and generate timestamped log filename
        mkdir -p "$INSTALL_LOC/logs"
        LOG_FILE="$INSTALL_LOC/logs/$(date +%Y%m%d_%H%M%S).log"
        printf "├─${DIM} Logs: %s${NC}\n" "$LOG_FILE"

        display_enterprise_server_command false

        # Start server in background
        "$INSTALL_LOC/$BINARY_NAME" serve --cluster-id="$CLUSTER_ID" --node-id="$NODE_ID" --license-type="$LICENSE_TYPE" --license-email="$LICENSE_EMAIL" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS >> "$LOG_FILE" 2>&1 &
        PID="$!"

        printf "├─${DIM} Server started in background (PID: %s)${NC}\n" "$PID"

        perform_server_health_check 90 true

    else
        printf "\n"
        printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed. Nice!${NC}\n"
    fi
fi

### SUCCESS INFORMATION ###
printf "\n"
if [ "${EDITION}" = "Enterprise" ] && [ "$SUCCESS" -eq 0 ] 2>/dev/null; then
    printf "${BOLD}Server startup failed${NC} - troubleshooting options:\n"
    printf "├─ ${BOLD}${YELLOW}Check email verification:${NC} ${BOLD}Look for verification email and click the link${NC}\n"
    printf "├─ ${BOLD}   → This is the most common issue for Enterprise activation${NC}\n"
    printf "├─ ${BOLD}Manual startup:${NC} Try running the server manually to see detailed logs:\n"
    printf "     influxdb3 serve \\\\\n"
    printf "     --cluster-id=%s \\\\\n" "${CLUSTER_ID:-cluster0}"
    printf "     --node-id=%s \\\\\n" "${NODE_ID:-node0}"
    printf "     --license-type=%s \\\\\n" "${LICENSE_TYPE:-trial}"
    printf "     --license-email=%s \\\\\n" "${LICENSE_EMAIL:-your@email.com}"
    printf "     %s\n" "${STORAGE_FLAGS_ECHO:-"--object-store=file --data-dir $INSTALL_LOC/data --plugin-dir $INSTALL_LOC/plugins"}"
    printf "└─ ${BOLD}Common issues:${NC} Network connectivity, invalid email format, port conflicts\n"
else
    printf "${BOLD}Next Steps${NC}\n"
    if [ -n "$shellrc" ]; then
        printf "├─ Run ${BOLD}source '%s'${NC}, then access InfluxDB with ${BOLD}influxdb3${NC} command.\n" "$shellrc"
    else
        printf "├─ Access InfluxDB with the ${BOLD}influxdb3${NC} command.\n"
    fi
    printf "├─ Create admin token: ${BOLD}influxdb3 create token --admin${NC}\n"
    printf "└─ Begin writing data! Learn more at https://docs.influxdata.com/influxdb3/${EDITION_TAG}/get-started/write/\n\n"
fi

printf "┌────────────────────────────────────────────────────────────────────────────────────────┐\n"
printf "│ Looking to use a UI for querying, plugins, management, and more?                       │\n"
printf "│ Get InfluxDB 3 Explorer at ${BLUE}https://docs.influxdata.com/influxdb3/explorer/#quick-start${NC} │\n"
printf "└────────────────────────────────────────────────────────────────────────────────────────┘\n\n"

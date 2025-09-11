#!/bin/sh -e

# ==========================Script Config==========================

readonly GREEN='\033[0;32m'
readonly BLUE='\033[0;34m'
readonly BOLD='\033[1m'
readonly BOLDGREEN='\033[1;32m'
readonly DIM='\033[2m'
readonly NC='\033[0m' # No Color

# No diagnostics for: 'printf "...${FOO}"'
# shellcheck disable=SC2059

ARCHITECTURE=$(uname -m)
ARTIFACT=""
OS=""
INSTALL_LOC=~/.influxdb3
BINARY_NAME="influxdb3"
PORT=8181

INFLUXDB_VERSION="3.4.1"
EDITION="Core"
EDITION_TAG="core"
if [ "$1" = "enterprise" ]; then
    EDITION="Enterprise"
    EDITION_TAG="enterprise"
    shift 1
fi



# ==========================Detect OS/Architecture==========================

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



# ==========================Reusable Script Functions ==========================

# Function to find available port
find_available_port() {
    local show_progress="${1:-true}"
    lsof_exec=$(command -v lsof) && {
        while [ -n "$lsof_exec" ] && lsof -i:"$PORT" -t >/dev/null 2>&1; do
            if [ "$show_progress" = "true" ]; then
                printf "├─${DIM} Port %s is in use. Finding new port.${NC}\n" "$PORT"
            fi
            PORT=$((PORT + 1))
            if [ "$PORT" -gt 32767 ]; then
                printf "└─${DIM} Could not find an available port. Aborting.${NC}\n"
                exit 1
            fi
            if ! "$lsof_exec" -i:"$PORT" -t >/dev/null 2>&1; then
                if [ "$show_progress" = "true" ]; then
                    printf "└─${DIM} Found an available port: %s${NC}\n" "$PORT"
                fi
                break
            fi
        done
    }
}

# Function to set up Quick Start defaults for both Core and Enterprise
setup_quick_start_defaults() {
    local edition="${1:-core}"
    
    NODE_ID="node0" 
    STORAGE_TYPE="File Storage"
    STORAGE_PATH="$HOME/.influxdb3/data"
    PLUGIN_PATH="$HOME/.influxdb3/plugins"
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

# Function to configure AWS S3 storage
configure_aws_s3_storage() {
    echo
    printf "${BOLD}AWS S3 Configuration${NC}\n"
    printf "├─ Enter AWS Access Key ID: "
    read -r AWS_KEY

    printf "├─ Enter AWS Secret Access Key: "
    stty -echo
    read -r AWS_SECRET
    stty echo

    echo
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
    echo
    printf "${BOLD}Azure Storage Configuration${NC}\n"
    printf "├─ Enter Storage Account Name: "
    read -r AZURE_ACCOUNT

    printf "└─ Enter Storage Access Key: "
    stty -echo
    read -r AZURE_KEY
    stty echo

    echo
    STORAGE_FLAGS="--object-store=azure --azure-storage-account=${AZURE_ACCOUNT}"
    STORAGE_FLAGS_ECHO="$STORAGE_FLAGS --azure-storage-access-key=..."
    STORAGE_FLAGS="$STORAGE_FLAGS --azure-storage-access-key=${AZURE_KEY}"
}

# Function to configure Google Cloud storage  
configure_google_cloud_storage() {
    echo
    printf "${BOLD}Google Cloud Storage Configuration${NC}\n"
    printf "└─ Enter path to service account JSON file: "
    read -r GOOGLE_SA
    STORAGE_FLAGS="--object-store=google --google-service-account=${GOOGLE_SA}"
    STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
}

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
        echo
        printf "${BOLD}License Setup Required${NC}\n"
        printf "1) ${GREEN}Trial${NC} ${DIM}- Full features for 30 days (up to 256 cores)${NC}\n"
        printf "2) ${GREEN}Home${NC} ${DIM}- Free for non-commercial use (max 2 cores, single node)${NC}\n"
        echo
        printf "Enter choice (1-2): "
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

# Function to prompt for storage configuration
prompt_storage_configuration() {
    # Prompt for storage solution
    echo
    printf "${BOLD}Select Your Storage Solution${NC}\n"
    printf "├─ 1) File storage (Persistent)\n"
    printf "├─ 2) Object storage (Persistent)\n"
    printf "├─ 3) In-memory storage (Non-persistent)\n"
    printf "└─ Enter your choice (1-3): "
    read -r STORAGE_CHOICE

    case "$STORAGE_CHOICE" in
        1)
            STORAGE_TYPE="File Storage"
            echo
            printf "Enter storage path (default: %s/data): " "${INSTALL_LOC}"
            read -r STORAGE_PATH
            STORAGE_PATH=${STORAGE_PATH:-"${INSTALL_LOC}/data"}
            STORAGE_FLAGS="--object-store=file --data-dir ${STORAGE_PATH}"
            STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
            ;;
        2)
            STORAGE_TYPE="Object Storage"
            echo
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
                    STORAGE_FLAGS="--object-store=file --data-dir ${INSTALL_LOC}/data"
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
            STORAGE_FLAGS="--object-store=file --data-dir ${INSTALL_LOC}/data"
            STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
            ;;
    esac
}

# Function to start server and perform health check
start_server_with_health_check() {
    local timeout_seconds="${1:-30}"
    local is_enterprise="${2:-false}"
    local show_progress="${3:-true}"
    
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
                printf "├─${DIM} Server process stopped unexpectedly${NC}\n"
            fi
            break
        fi

        if curl --max-time 3 -s "http://localhost:$PORT/health" >/dev/null 2>&1; then
            printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed and running on port %s. Nice!${NC}\n" "$PORT"
            SUCCESS=1
            break
        fi
        
        # Show email verification message after 5 seconds for Enterprise
        if [ "$is_enterprise" = "true" ] && [ "$i" -eq 5 ] && [ "$EMAIL_MESSAGE_SHOWN" = "false" ]; then
            printf "├─${DIM} Checking license activation - please verify your email${NC}\n"
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
            printf "└─${BOLD} ERROR: InfluxDB Enterprise failed to start within %s seconds${NC}\n" "$timeout_seconds"
            if [ "$show_progress" = "true" ]; then
                printf "   This may be due to:\n"
                printf "   ├─ Email verification required (check your email)\n"
                printf "   ├─ Network connectivity issues during license retrieval\n"
                printf "   ├─ Invalid license type or email format\n"
                printf "   ├─ Port %s already in use\n" "$PORT"
                printf "   └─ Server startup issues\n"
            else
                if [ -n "$LICENSE_TYPE" ]; then
                    printf "   ├─ Check your email for license verification if required\n"
                fi
                printf "   ├─ Network connectivity issues\n"
                printf "   └─ Port %s conflicts\n" "$PORT"
            fi
            
            # Kill the background process if it's still running
            if kill -0 "$PID" 2>/dev/null; then
                printf "   Stopping background server process...\n"
                kill "$PID" 2>/dev/null
            fi
        else
            printf "└─${BOLD} ERROR: InfluxDB failed to start; check permissions or other potential issues.${NC}\n"
            exit 1
        fi
    fi
}

# Function to display Enterprise server command
display_enterprise_server_command() {
    local is_quick_start="${1:-false}"
    
    if [ "$is_quick_start" = "true" ]; then
        # Quick Start format
        printf "└─${DIM} Command: ${NC}\n"
        printf "${DIM}   influxdb3 serve \\\\${NC}\n"
        printf "${DIM}    --cluster-id=%s \\\\${NC}\n" "$CLUSTER_ID"
        printf "${DIM}    --node-id=%s \\\\${NC}\n" "$NODE_ID"
        if [ -n "$LICENSE_TYPE" ] && [ -n "$LICENSE_EMAIL" ]; then
            printf "${DIM}    --license-type=%s \\\\${NC}\n" "$LICENSE_TYPE"
            printf "${DIM}    --license-email=%s \\\\${NC}\n" "$LICENSE_EMAIL"
        fi
        printf "${DIM}    --http-bind=0.0.0.0:%s \\\\${NC}\n" "$PORT"
        printf "${DIM}    %s${NC}\n" "$STORAGE_FLAGS_ECHO"
        echo
    else
        # Custom configuration format
        printf "│\n"
        printf "├─ Running serve command:\n"
        printf "├─${DIM} influxdb3 serve \\\\${NC}\n"
        printf "├─${DIM}   --cluster-id='%s' \\\\${NC}\n" "$CLUSTER_ID"
        printf "├─${DIM}   --node-id='%s' \\\\${NC}\n" "$NODE_ID"
        printf "├─${DIM}   --license-type='%s' \\\\${NC}\n" "$LICENSE_TYPE"
        printf "├─${DIM}   --license-email='%s' \\\\${NC}\n" "$LICENSE_EMAIL"
        printf "├─${DIM}   --http-bind='0.0.0.0:%s' \\\\${NC}\n" "$PORT"
        printf "├─${DIM}   %s${NC}\n" "$STORAGE_FLAGS_ECHO"
        printf "│\n"
    fi
}



# =========================Installation==========================

# Attempt to clear screen and show welcome message
clear 2>/dev/null || true  # clear isn't available everywhere
printf "┌───────────────────────────────────────────────────┐\n"
printf "│ ${BOLD}Welcome to InfluxDB!${NC} We'll make this quick.       │\n"
printf "└───────────────────────────────────────────────────┘\n"

echo
printf "${BOLD}Select Installation Type${NC}\n"
echo
printf "1) ${GREEN}Docker Image${NC}    ${DIM}(The official Docker image)${NC}\n"
printf "2) ${GREEN}Simple Download${NC} ${DIM}(No dependencies required)${NC}\n"
echo
printf "Enter your choice (1-2): "
read -r INSTALL_TYPE

case "$INSTALL_TYPE" in
    1)
        printf "\n\n${BOLD}Download and Tag Docker Image${NC}\n"
        printf "├─ ${DIM}docker pull influxdb:${EDITION_TAG}${NC}\n"
        printf "└─ ${DIM}docker tag influxdb:${EDITION_TAG} influxdb3-${EDITION_TAG}${NC}\n\n"
        if ! docker pull "influxdb:3-${EDITION_TAG}"; then
            printf "└─ Error: Failed to download Docker image.\n"
            exit 1
        fi
        docker tag influxdb:3-${EDITION_TAG} influxdb3-${EDITION_TAG}
        # Exit script after Docker installation
        echo
        printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} successfully pulled. Nice!${NC}\n\n"
        printf "${BOLD}NEXT STEPS${NC}\n"
        printf "1) Run the Docker image:\n"
        printf "   └─ ${BOLD}docker run -it -p 8181:8181 --name influxdb3-container \\"
        printf "\n      --volume ~/.influxdb3_data:/.data --volume ~/.influxdb3_plugins:/plugins influxdb:3-${EDITION_TAG} \\"
        printf "\n      influxdb3 serve"
        if [ "${EDITION}" = "Enterprise" ]; then
            printf " --cluster-id c0"
        fi
        printf " --node-id node0 --object-store file --data-dir /.data --plugin-dir /plugins${NC}\n\n"
        printf "2) ${NC}Create a token: ${BOLD}docker exec -it influxdb3-container influxdb3 create token --admin${NC} \n\n"
        printf "3) Begin writing data! Learn more at https://docs.influxdata.com/influxdb3/${EDITION_TAG}/get-started/write/\n\n"
        printf "┌────────────────────────────────────────────────────────────────────────────────────────┐\n"
        printf "│ Looking to use a UI for querying, plugins, management, and more?                       │\n"
        printf "│ Get InfluxDB 3 Explorer at ${BLUE}https://docs.influxdata.com/influxdb3/explorer/#quick-start${NC} │\n"
        printf "└────────────────────────────────────────────────────────────────────────────────────────┘\n\n"
        exit 0
        ;;
    2)
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

echo
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
    printf " (ERROR: %s != %s). Aborting.${NC}\n" "$ch_sha" "$dl_sha"
    exit 1
fi
printf "└─${DIM} rm '%s/influxdb3-${EDITION_TAG}.tar.gz.sha256'${NC}\n" "$INSTALL_LOC"
rm "$INSTALL_LOC/influxdb3-${EDITION_TAG}.tar.gz.sha256"

echo
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
    echo
    printf "${BOLD}Adding InfluxDB to '%s'${NC}\n" "$shellrc"
    printf "└─${DIM} export PATH=\"\$PATH:%s/\" >> '%s'${NC}\n" "$INSTALL_LOC" "$shellrc"
    echo "export PATH=\"\$PATH:$INSTALL_LOC/\"" >> "$shellrc"
fi

if [ "${EDITION}" = "Core" ]; then
    # Prompt user for startup options
    echo
    printf "${BOLD}What would you like to do next?${NC}\n"
    printf "1) ${GREEN}Quick Start${NC} ${DIM}(recommended; data stored at ~/.influxdb3/data)${NC}\n"
    printf "2) ${GREEN}Custom Configuration${NC} ${DIM}(configure all options manually)${NC}\n"
    printf "3) ${GREEN}Skip startup${NC} ${DIM}(install only)${NC}\n"
    echo
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
        echo
        printf "${BOLD}Enter Your Node ID${NC}\n"
        printf "├─ A Node ID is a unique, uneditable identifier for a service.\n"
        printf "└─ Enter a Node ID (default: node0): "
        read -r NODE_ID
        NODE_ID=${NODE_ID:-node0}

        # Prompt for storage solution
        prompt_storage_configuration

        # Ensure port is available; if not, find a new one.
        find_available_port

        # Start and give up to 30 seconds to respond
        echo
        printf "${BOLD}Starting InfluxDB${NC}\n"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} Storage: %s${NC}\n" "$STORAGE_TYPE"
        printf "├─${DIM} influxdb3 serve \\\\${NC}\n"
        printf "├─${DIM}   --node-id='%s' \\\\${NC}\n" "$NODE_ID"
        printf "├─${DIM}   --http-bind='0.0.0.0:%s' \\\\${NC}\n" "$PORT"
        printf "└─${DIM}   %s${NC}\n" "$STORAGE_FLAGS_ECHO"
        "$INSTALL_LOC/$BINARY_NAME" serve --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS > /dev/null &
        PID="$!"

        start_server_with_health_check 30

    elif [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "1" ]; then
        # Quick Start flow - minimal output, just start the server
        echo
        printf "${BOLD}Starting InfluxDB (Quick Start)${NC}\n"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} Storage: ~/.influxdb3/data${NC}\n"
        printf "├─${DIM} Plugins: ~/.influxdb3/plugins${NC}\n"

        # Ensure port is available; if not, find a new one.
        ORIGINAL_PORT="$PORT"
        find_available_port false
        
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

        # Start server in background
        "$INSTALL_LOC/$BINARY_NAME" serve --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS > /dev/null &
        PID="$!"

        start_server_with_health_check 30

    else
        echo
        printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed. Nice!${NC}\n"
    fi
else
    # Enterprise startup options
    echo
    printf "${BOLD}What would you like to do next?${NC}\n"
    printf "1) ${GREEN}Quick Start${NC} ${DIM}(recommended; data stored at ~/.influxdb3/data)${NC}\n"
    printf "2) ${GREEN}Custom Configuration${NC} ${DIM}(configure all options manually)${NC}\n"
    printf "3) ${GREEN}Skip startup${NC} ${DIM}(install only)${NC}\n"
    echo
    printf "Enter your choice (1-3): "
    read -r STARTUP_CHOICE
    STARTUP_CHOICE=${STARTUP_CHOICE:-1}

    case "$STARTUP_CHOICE" in
        1)
            # Quick Start - use defaults and check for existing license
            setup_quick_start_defaults enterprise
            setup_license_for_quick_start
            
            STORAGE_FLAGS="--object-store=file --data-dir ${STORAGE_PATH} --plugin-dir ${PLUGIN_PATH}"
            STORAGE_FLAGS_ECHO="--object-store=file --data-dir ${STORAGE_PATH} --plugin-dir ${PLUGIN_PATH}"
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
        *)
            printf "Invalid choice. Using Quick Start (option 1).\n"
            # Same as option 1
            setup_quick_start_defaults enterprise
            setup_license_for_quick_start
            
            STORAGE_FLAGS="--object-store=file --data-dir ${STORAGE_PATH} --plugin-dir ${PLUGIN_PATH}"
            STORAGE_FLAGS_ECHO="--object-store=file --data-dir ${STORAGE_PATH} --plugin-dir ${PLUGIN_PATH}"
            START_SERVICE="y"
            ;;
    esac

    if [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "1" ]; then
        # Enterprise Quick Start flow
        echo
        printf "${BOLD}Starting InfluxDB Enterprise (Quick Start)${NC}\n"
        printf "├─${DIM} Cluster ID: %s${NC}\n" "$CLUSTER_ID"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        if [ -n "$LICENSE_TYPE" ]; then
            printf "├─${DIM} License Type: %s${NC}\n" "$LICENSE_DESC"
        fi
        if [ -n "$LICENSE_EMAIL" ]; then
            printf "├─${DIM} Email: %s${NC}\n" "$LICENSE_EMAIL"
        fi
        printf "├─${DIM} Storage: ~/.influxdb3/data${NC}\n"
        printf "├─${DIM} Plugins: ~/.influxdb3/plugins${NC}\n"

        # Ensure port is available; if not, find a new one.
        ORIGINAL_PORT="$PORT"
        find_available_port false
        
        # Show port result
        if [ "$PORT" != "$ORIGINAL_PORT" ]; then
            printf "├─${DIM} Found available port: %s (%s-%s in use)${NC}\n" "$PORT" "$ORIGINAL_PORT" "$((PORT - 1))"
        fi
        
        # Show the command being executed
        display_enterprise_server_command true

        # Start server in background with or without license flags
        if [ -n "$LICENSE_TYPE" ] && [ -n "$LICENSE_EMAIL" ]; then
            # New license needed
            "$INSTALL_LOC/$BINARY_NAME" serve --cluster-id="$CLUSTER_ID" --node-id="$NODE_ID" --license-type="$LICENSE_TYPE" --license-email="$LICENSE_EMAIL" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS > /dev/null 2>&1 &
        else
            # Existing license file
            "$INSTALL_LOC/$BINARY_NAME" serve --cluster-id="$CLUSTER_ID" --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS > /dev/null 2>&1 &
        fi
        PID="$!"
        
        start_server_with_health_check 90 true false

    elif [ "$START_SERVICE" = "y" ] && [ "$STARTUP_CHOICE" = "2" ]; then
        # Enterprise Custom Start flow
        echo
        # Prompt for Cluster ID
        printf "${BOLD}Enter Your Cluster ID${NC}\n"
        printf "├─ A Cluster ID determines part of the storage path hierarchy.\n"
        printf "├─ All nodes within the same cluster share this identifier.\n"
        printf "└─ Enter a Cluster ID (default: cluster0): "
        read -r CLUSTER_ID
        CLUSTER_ID=${CLUSTER_ID:-cluster0}

        # Prompt for Node ID
        echo
        printf "${BOLD}Enter Your Node ID${NC}\n"
        printf "├─ A Node ID distinguishes individual server instances within the cluster.\n"
        printf "└─ Enter a Node ID (default: node0): "
        read -r NODE_ID
        NODE_ID=${NODE_ID:-node0}

        # Prompt for license type
        echo
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
        echo
        printf "${BOLD}Enter Your Email Address${NC}\n"
        printf "├─ Required for license verification and activation\n"
        printf "├─ You may need to check your email for verification\n"
        printf "└─ Email: "
        read -r LICENSE_EMAIL
        
        while [ -z "$LICENSE_EMAIL" ]; do
            printf "├─ Email address is required. Please enter your email: "
            read -r LICENSE_EMAIL
        done

        # Prompt for storage solution
        prompt_storage_configuration

        # Ensure port is available; if not, find a new one.
        find_available_port

        # Start Enterprise in background with licensing and give up to 60 seconds to respond (licensing takes longer)
        echo
        printf "${BOLD}Starting InfluxDB Enterprise${NC}\n"
        printf "├─${DIM} Cluster ID: %s${NC}\n" "$CLUSTER_ID"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} License Type: %s${NC}\n" "$LICENSE_DESC"
        printf "├─${DIM} Email: %s${NC}\n" "$LICENSE_EMAIL"
        printf "├─${DIM} Storage: %s${NC}\n" "$STORAGE_TYPE"
        display_enterprise_server_command false
        
        # Start server in background
        "$INSTALL_LOC/$BINARY_NAME" serve --cluster-id="$CLUSTER_ID" --node-id="$NODE_ID" --license-type="$LICENSE_TYPE" --license-email="$LICENSE_EMAIL" --http-bind="0.0.0.0:$PORT" $STORAGE_FLAGS > /dev/null 2>&1 &
        PID="$!"
        
        printf "├─${DIM} Server started in background (PID: %s)${NC}\n" "$PID"
        printf "├─${DIM} Checking license activation and server health...${NC}\n"
        
        start_server_with_health_check 90 true true

    else
        echo
        printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed. Nice!${NC}\n"
    fi
fi

### SUCCESS INFORMATION ###
echo
printf "${BOLD}Next Steps${NC}\n"
if [ -n "$shellrc" ]; then
    printf "1) Run ${BOLD}source '%s'${NC}, then access InfluxDB with ${BOLD}influxdb3${NC} command.\n\n" "$shellrc"
else
    printf "1) Access InfluxDB with the ${BOLD}influxdb3${NC} command.\n"
fi
if [ "${EDITION}" = "Enterprise" ] && [ "$SUCCESS" -eq 0 ] 2>/dev/null; then
    printf "├─ ${BOLD}Server startup failed${NC} - troubleshooting options:\n"
    printf "└─ ${BOLD}Check email verification:${NC} Look for verification email and click the link\n\n"
    printf "1) ${BOLD}Manual startup:${NC} Try running the server manually to see detailed logs:\n"
    printf "     influxdb3 serve \\\\\n"
    printf "       --cluster-id=%s \\\\\n" "${CLUSTER_ID:-cluster0}"
    printf "       --node-id=%s \\\\\n" "${NODE_ID:-node0}"
    printf "       --license-type=%s \\\\\n" "${LICENSE_TYPE:-trial}"
    printf "       --license-email=%s \\\\\n" "${LICENSE_EMAIL:-your@email.com}"
    printf "       %s\n" "${STORAGE_FLAGS_ECHO:-"--object-store=file --data-dir ~/.influxdb3/data --plugin-dir ~/.influxdb3/plugins"}"
    printf "   ${BOLD}Common issues:${NC} Network connectivity, invalid email format, port conflicts\n"
fi

printf "2) ${BOLD}Create admin token:${NC} influxdb3 create token --admin\n\n"
printf "3) ${BOLD}Set token:${NC} export INFLUXDB3_AUTH_TOKEN=<your_token>\n\n"

printf "View the Getting Started guide at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/get-started/${NC}.\n"
printf "Visit our public Discord at \033[4;94mhttps://discord.gg/az4jPm8x${NC} for additional guidance.\n"
echo

printf "┌────────────────────────────────────────────────────────────────────────────────────────┐\n"
printf "│ Looking to use a UI for querying, plugins, management, and more?                       │\n"
printf "│ Get InfluxDB 3 Explorer at ${BLUE}https://docs.influxdata.com/influxdb3/explorer/#quick-start${NC} │\n"
printf "└────────────────────────────────────────────────────────────────────────────────────────┘\n\n"
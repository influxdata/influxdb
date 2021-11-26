#!/bin/bash

set -eu -o pipefail

# extract ordinal index from server ID
[[ $HOSTNAME =~ -([0-9]+)$ ]] || (echo "invalid hostname" && exit 1)
ordinal=${BASH_REMATCH[1]}

# calculate server ID
offset="${INFLUXDB_IOX_ID_OFFSET:-0}"
server_id=$((ordinal + offset))
echo "ServerID: $server_id"

# set server ID
while true; do
    set +e
    grpcurl -d "{\"id\": $server_id}" -allow-unknown-fields -plaintext "$INFLUXDB_IOX_GRPC_BIND_ADDR" influxdata.iox.deployment.v1.DeploymentService.UpdateServerId
    status=$?
    set -e

    if [[ $status == 0 ]]; then
        echo "server ID set"
        break
    else
        echo "cannot set server ID yet, waiting..."
        sleep 1
    fi
done

# wait for rule/config updates
FINGERPRINT_DONE=/fingerprint.done
FINGERPRINT_STAGING=/fingerprint.staging
F_CURRENT=/current
while true; do
    # create new fingerprint
    rm -rf "$FINGERPRINT_STAGING"
    mkdir "$FINGERPRINT_STAGING"

    for cfg_file in /iox_config/*; do
        # create backup of file so it doesn't change while we're working with it
        cp "$cfg_file" "$F_CURRENT"

        # determine type
        if [[ $cfg_file == *"router"* ]]; then
            type="router"
        else
            type="database"
        fi

        # compare hash
        hash="$(sha256sum "$F_CURRENT" | sed -E 's/([a-f0-9]+).*/\1/g')"
        in_sync=0
        if [ -f "$FINGERPRINT_DONE/$hash.$type" ]; then
            echo "$cfg_file: in-sync"
            in_sync=1
        else
            echo "$cfg_file: out of sync"

            # select create/update routing depending on the config type
            if [[ $type == "database" ]]; then
                echo "Create database..."

                set +e
                grpcurl -d @ < "$F_CURRENT" -allow-unknown-fields -plaintext "$INFLUXDB_IOX_GRPC_BIND_ADDR" influxdata.iox.management.v1.ManagementService.CreateDatabase
                status=$?
                set -e

                if [[ $status == 0 ]]; then
                    echo "databse created"
                    in_sync=1
                else
                    echo "cannot create database, try updating it..."

                    set +e
                    grpcurl -d @ < "$F_CURRENT" -allow-unknown-fields -plaintext "$INFLUXDB_IOX_GRPC_BIND_ADDR" influxdata.iox.management.v1.ManagementService.UpdateDatabase
                    status=$?
                    set -e

                    if [[ $status == 0 ]]; then
                        echo "database updated"
                        in_sync=1
                    else
                        echo "cannot update database"
                    fi
                fi
            else
                echo "Update router..."

                set +e
                grpcurl -d @ < "$F_CURRENT" -allow-unknown-fields -plaintext "$INFLUXDB_IOX_GRPC_BIND_ADDR" influxdata.iox.router.v1.RouterService.UpdateRouter
                status=$?
                set -e

                if [[ $status == 0 ]]; then
                    echo "router updated"
                    in_sync=1
                else
                    echo "cannot update router"
                fi
            fi
        fi

        # store state
        if [[ $in_sync == 1 ]]; then
            cp "$F_CURRENT" "$FINGERPRINT_STAGING/$hash.$type"
        fi
    done

    # TODO: delete resources that are no longer present, should be done by comparing the list of resources on the server VS the ones in config files by name

    # store fingerprints for next round
    rm -rf "$FINGERPRINT_DONE"
    mkdir "$FINGERPRINT_DONE"
    cp -r "$FINGERPRINT_STAGING"/. "$FINGERPRINT_DONE"/

    sleep 10
done

#!/bin/sh

# This script requests a license from a locally running InfluxDB 3 license service.
# The license service must be running on the default port 8687.

# Parse the command line arguments.
while getopts "e:w:i:" opt; do
  case $opt in
    e) email=$OPTARG ;;
    w) writer_id=$OPTARG ;;
    i) instance_id=$OPTARG ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

# Check that the required email and writer_id arguments are present.
if [ -z "$email" ] || [ -z "$writer_id" ]; then
  echo "Usage: $0 -e email -w writer_id [-i instance_id]" >&2
  exit 1
fi

# If the instance_id argument is not present, generate a new UUID.
if [ -z "$instance_id" ]; then
  instance_id=`uuidgen`
fi

# Send the POST request to the license service.
resp=$(curl -s -w '@-' -X POST "http://localhost:8687/licenses" \
     --data-urlencode "email=${email}" \
     -d "writer-id=${writer_id}" \
     -d "instance-id=${instance_id}" <<EOF
{
    "http_code": %{http_code},
    "headers": %{header_json}
}
EOF
)

# Check the response for an error.
if [ $? -ne 0 ]; then
  echo "Error: curl failed" >&2
  exit 1
fi

# Expect 201 Created HTTP status code.
http_code=$(echo "$resp" | jq -r '.http_code')
if [ "$http_code" != "201" ]; then
  echo "Error: HTTP status code $http_code" >&2
  exit 1
fi

# Extract the Location header from the response.
location=$(echo "$resp" | jq -r '.headers.location[0]')
if [ -z "$location" ]; then
  echo "Error: Location header not found" >&2
  exit 1
fi

# Use the Location header to poll the license service until the license is ready.
echo "Click the verification link in the email (or from the logs or database) to complete the license request."
printf "Waiting for the license to be ready"
while true; do
  body_file=$(mktemp)
  resp=$(curl -s -o "$body_file" -w '@-' -X GET "http://localhost:8687${location}" << 'EOF'
{
    "http_code": %{http_code}
}
EOF
)

  # Check the response for an error.
  if [ $? -ne 0 ]; then
    echo "Error: curl failed" >&2
    exit 1
  fi

  # Expect 200 OK HTTP status code.
  http_code=$(echo "$resp" | jq -r '.http_code')
  # If 404 Not Found, the license is not ready yet.
  if [ "$http_code" == "404" ]; then
    # Update visual progress while waiting for the license to be ready.
    printf "."
    sleep 1
    continue
  elif [ "$http_code" != "200" ]; then
    echo "Error: HTTP status code $http_code" >&2
    exit 1
  fi

  # Extract the response body from the response.
  response_body=$(cat "$body_file")
  rm "$body_file"

  # If the response body is null or empty, something went wrong.
  if [ -z "$response_body" ] || [ "$response_body" == "null" ]; then
    echo "Error: Response body not found" >&2
     exit 1
  fi

  # Print the license
  echo "\n$response_body"
  break
done
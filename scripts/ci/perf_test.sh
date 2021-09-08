set -ex -o pipefail

case "${TEST_FORMAT}" in
  http|flux-http)
    ;;
  *)
    >&2 echo "Unknown query format: ${TEST_FORMAT}"
    exit 1
    ;;
esac

# get latest ubuntu 20.04 ami for us-west-2
ami_id=$(aws --region us-west-2 ssm get-parameters --names /aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id --query 'Parameters[0].[Value]' --output text)

# launch ec2 instance
instance_type="r5.2xlarge"
datestring=$(date +%Y%m%d)
instance_info=$(aws --region us-west-2 ec2 run-instances \
  --image-id $ami_id \
  --instance-type $instance_type \
  --block-device-mappings DeviceName=/dev/sda1,Ebs={VolumeSize=200} \
  --key-name circleci-oss-test \
  --security-group-ids sg-03004366a38eccc97 \
  --subnet-id subnet-0c079d746f27ede5e \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=oss-perftest-$datestring-${CIRCLE_BRANCH}-${CIRCLE_SHA1}-${TEST_FORMAT}}]")

# get instance info
ec2_instance_id=$(echo $instance_info | jq -r .Instances[].InstanceId)

# pull down the latest influx_tools
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} aws --region us-west-2 s3 cp s3://perftest-binaries-influxdb/influx_tools/latest_1.8.txt ./latest.txt
latest_tools=$(cat latest.txt | cut -d ' ' -f1)
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} aws --region us-west-2 s3 cp s3://perftest-binaries-influxdb/influx_tools/$latest_tools ./influx_tools
sleep 60

ec2_ip=$(aws \
  --region us-west-2 \
  ec2 describe-instances \
    --instance-ids $ec2_instance_id \
    --query "Reservations[].Instances[].PublicIpAddress" \
    --output text)

while [ -z $ec2_ip ]; do
  sleep 5
  ec2_ip=$(aws \
    --region us-west-2 \
    ec2 describe-instances \
      --instance-ids $ec2_instance_id \
      --query "Reservations[].Instances[].PublicIpAddress" \
      --output text)
done

trap "aws --region us-west-2 ec2 terminate-instances --instance-ids $ec2_instance_id" KILL

# push binary and script to instance
debname=$(find /tmp/workspace/dist/influxd_linux_amd64/influxdb2*amd64.deb)
base_debname=$(basename $debname)
source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

scp influx_tools ubuntu@$ec2_ip:/home/ubuntu/influx_tools
scp $debname ubuntu@$ec2_ip:/home/ubuntu/$base_debname
scp ${source_dir}/run_perftest.sh ubuntu@$ec2_ip:/home/ubuntu/run_perftest.sh

# install deb in remote vm and create ramdisk for dataset files
RAMDISK_DIR=/mnt/ramdisk
ssh ubuntu@$ec2_ip << EOF
sudo chmod +x /home/ubuntu/influx_tools
sudo DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes /home/ubuntu/$base_debname
sudo systemctl unmask influxdb.service
sudo systemctl start influxdb
sudo mkdir -p ${RAMDISK_DIR}
sudo mount -t tmpfs -o size=32G tmpfs ${RAMDISK_DIR}
EOF

# setup influxdb2
export INFLUXDB2=true
export TEST_ORG=example_org
export TEST_TOKEN=token
result=$(ssh ubuntu@$ec2_ip "curl -s -o /dev/null -H \"Content-Type: application/json\" -XPOST -d '{\"username\": \"default\", \"password\": \"thisisnotused\", \"retentionPeriodSeconds\": 0, \"org\": \"$TEST_ORG\", \"bucket\": \"unused_bucket\", \"token\": \"$TEST_TOKEN\"}' http://localhost:8086/api/v2/setup -w %{http_code}")
if [ "$result" != "201" ]; then
  echo "Influxdb2 failed to setup correctly"
  exit 1
fi

# run tests
set +x
export COMMIT_TIME=$(git show -s --format=%ct)
echo "running 'ssh ubuntu@$ec2_ip \"nohup sudo AWS_ACCESS_KEY_ID=REDACTED AWS_SECRET_ACCESS_KEY=REDACTED CIRCLE_TEARDOWN=true CIRCLE_TOKEN=REDACTED  CLOUD2_BUCKET=${CLOUD2_PERFTEST_BUCKET} CLOUD2_ORG=${CLOUD2_PERFTEST_ORG} DATASET_DIR=${RAMDISK_DIR} DATA_I_TYPE=${instance_type} DB_TOKEN=REDACTED INFLUXDB2=${INFLUXDB2} INFLUXDB_VERSION=${CIRCLE_BRANCH} TEST_FORMAT=${TEST_FORMAT} TEST_RECORD_INGEST_RESULTS=${TEST_RECORD_INGEST_RESULTS} NGINX_HOST=localhost TEST_COMMIT=${CIRCLE_SHA1} TEST_COMMIT_TIME=${COMMIT_TIME} TEST_ORG=${TEST_ORG} TEST_TOKEN=${TEST_TOKEN} CIRCLE_TEARDOWN_DATESTRING=$datestring ./run_perftest.sh > /home/ubuntu/perftest_log.txt 2>&1 &\"'"
ssh ubuntu@$ec2_ip "nohup sudo AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} CIRCLE_TEARDOWN=true CIRCLE_TOKEN=${CIRCLE_API_CALLBACK_TOKEN}  CLOUD2_BUCKET=${CLOUD2_PERFTEST_BUCKET} CLOUD2_ORG=${CLOUD2_PERFTEST_ORG} DATASET_DIR=${RAMDISK_DIR} DATA_I_TYPE=${instance_type} DB_TOKEN=${CLOUD2_PERFTEST_TOKEN} INFLUXDB2=${INFLUXDB2} INFLUXDB_VERSION=${CIRCLE_BRANCH} TEST_FORMAT=${TEST_FORMAT} TEST_RECORD_INGEST_RESULTS=${TEST_RECORD_INGEST_RESULTS} NGINX_HOST=localhost TEST_COMMIT=${CIRCLE_SHA1} TEST_COMMIT_TIME=${COMMIT_TIME} TEST_ORG=${TEST_ORG} TEST_TOKEN=${TEST_TOKEN} CIRCLE_TEARDOWN_DATESTRING=$datestring ./run_perftest.sh > /home/ubuntu/perftest_log.txt 2>&1 &"


set -ex -o pipefail

source vars.sh

# get latest ubuntu 21.10 ami for us-west-2
ami_id=$(aws --region us-west-2 ssm get-parameters --names /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id --query 'Parameters[0].[Value]' --output text)

# launch ec2 instance
datestring=$(date +%Y%m%d)
instance_info=$(aws --region us-west-2 ec2 run-instances \
  --image-id $ami_id \
  --instance-type $DATA_I_TYPE \
  --block-device-mappings DeviceName=/dev/sda1,Ebs={VolumeSize=200} \
  --key-name circleci-oss-test \
  --security-group-ids sg-03004366a38eccc97 \
  --subnet-id subnet-0c079d746f27ede5e \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=oss-perftest-$datestring-${CIRCLE_BRANCH}-${CIRCLE_SHA1}}]")

# get instance info
ec2_instance_id=$(echo $instance_info | jq -r .Instances[].InstanceId)
echo "export EC2_INSTANCE_ID=$ec2_instance_id" >> vars.sh

ec2_ip=""
while [ -z $ec2_ip ]; do
  sleep 5
  ec2_ip=$(aws \
    --region us-west-2 \
    ec2 describe-instances \
      --instance-ids $ec2_instance_id \
      --query "Reservations[].Instances[].PublicIpAddress" \
      --output text)
done
echo "export EC2_IP=$ec2_ip" >> vars.sh

# push binary and script to instance
debname=$(find /tmp/workspace/artifacts/influxdb2*amd64.deb)
base_debname=$(basename $debname)
source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# On the first try, add the host key to the list of known hosts
until ssh -o StrictHostKeyChecking=no ubuntu@$ec2_ip echo Connected ; do
  echo Tried to ssh to ec2 instance, will retry
  sleep 5
done

scp $debname ubuntu@$ec2_ip:/home/ubuntu/$base_debname
scp ${source_dir}/run_perftest.sh ubuntu@$ec2_ip:/home/ubuntu/run_perftest.sh
scp -r ${source_dir}/perf-tests ubuntu@$ec2_ip:/home/ubuntu/perf-tests

echo "export TEST_COMMIT_TIME=$(git show -s --format=%ct)" >> vars.sh
echo "export CIRCLE_TEARDOWN=true" >> vars.sh
echo "export CIRCLE_TOKEN=${CIRCLE_API_CALLBACK_TOKEN}" >> vars.sh
echo "export CLOUD2_BUCKET=${CLOUD2_PERFTEST_BUCKET}" >> vars.sh
echo "export CLOUD2_ORG=${CLOUD2_PERFTEST_ORG}" >> vars.sh
echo "export DB_TOKEN=${CLOUD2_PERFTEST_TOKEN}" >> vars.sh
echo "export INFLUXDB_VERSION=${CIRCLE_BRANCH}" >> vars.sh
echo "export NGINX_HOST=localhost" >> vars.sh
echo "export TEST_COMMIT=${CIRCLE_SHA1}" >> vars.sh
scp vars.sh  ubuntu@$ec2_ip:/home/ubuntu/vars.sh

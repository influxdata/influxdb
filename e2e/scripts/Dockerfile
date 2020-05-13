#Use and official Python runtime as a base image
FROM quay.io/influxdb/influx:nightly

#ADD entrypoint.sh /entrypoint.sh

#Run app.py when the container launches
ENTRYPOINT ["/entrypoint.sh", "--e2e-testing=true"]

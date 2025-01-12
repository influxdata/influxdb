#### Summary

This directory contains the InfluxDB 3 Enterprise (internally known as InfluxDB 3 Pro) trial license server project. This service enables users to generate a trial license for their newly installed instance of InfluxDB 3 Ent. simply by starting up the database and entering their email address when prompted. InfluxDB then makes a request to this service for a trial license. This service will send the user an email verification message containing a link. Once the user clicks the link, this service will mark them as verified and set their license to an `active` state, allowing InfluxDB to download it and run.

This service is deployed using Google Cloud Run.

#### Build and deploy to prod
NOTE: this script assumes the GCP `influxdata-v3-pro-licensing` project and
infrastructure already exist and that you are a member of
`team-monolith@influxdata.com`. Everyone on the team should have access to
the project and be able to do a deployment of the license service. If the
project doesn't exist for some reason, it will have to be created. If the 
expected resource need to be deployed, see the "Creating or modifying project
infrastructure" section below (it's unlikely you need to do this).

Run the `release.sh` script. In a terminal and in the project root directory:
```
influxdb3_licensing/service/release.sh
```

#### Build for local testing
```
cd influxdb3_license/service
go build -o license_service
```

#### Setup local Postgres
First, you'll need Postgres running locally. There is a `docker-compose.postgres.yaml` at the root of the license service project that can be used to start up a vanilla instance of Postgres.

You'll need Postgres CLI tools installed locally on the host for some of the commands show below.
To install on a Mac:
```
brew install libqp
```
Then add the following to your `.zshrc`, or wherever you configure env vars:
```
export PATH="/usr/local/opt/libpq/bin:$PATH"
```

On Ubuntu/Debian, install the `postgresql-client` package.
On Fedora, RHEL/CentOS, or Arch, install `postgresql`

#### Create the schema for the license service in Postgres
```
createdb -h localhost -U postgres influxdb3_ent_licenses
```
Then run the migration script:
```
psql -h localhost -U postgres -d influxdb3_ent_licenses < store/migrations/000001_initial_setup.up.postgres.sql
```

#### Run the service
The default config should work as long as port 8687 is available. If it's not, you'll need to change the default config. To see available configuration options, run...
```
./license_service -h
```
To get...
```
Usage: license_service [flags]

Flags:
  -h, --help                                                                                                 Show context-sensitive help.
      --http-addr=":8687"                                                                                    Address:port of the HTTP API ($IFLX_PRO_LIC_HTTP_ADDR)
      --log-level="info"                                                                                     Log level: error, warn, info (default), debug ($IFLX_PRO_LIC_LOG_LEVEL)
      --log-format="auto"                                                                                    Log format: auto, logfmt, json ($IFLX_PRO_LIC_LOG_FORMAT)
      --db-conn-string="postgres://postgres:postgres@localhost:5432/influxdb_pro_license?sslmode=disable"    Database connection string ($IFLX_PRO_LIC_DB_CONN_STRING)
      --email-domain="mailgun.influxdata.com"                                                                Email domain name ($IFLX_PRO_LIC_EMAIL_DOMAIN)
      --email-api-key="log-only"                                                                             Email api key ($IFLX_PRO_LIC_EMAIL_API_KEY)
      --email-verification-url="http://localhost:8687"                                                       Email verification base URL ($IFLX_PRO_LIC_EMAIL_VERIFICATION_URL)
      --email-template-name="influxdb 3 enterprise verification"                                             Email template name ($IFLX_PRO_LIC_EMAIL_TEMPLATE_NAME)
      --email-max-retries=3                                                                                  Maximum number of email retries ($IFLX_PRO_LIC_EMAIL_MAX_RETRIES)
      --private-key="projects/influxdata-team-clustered/locations/global/keyRings/clustered-licensing/cryptoKeys/signing-key/cryptoKeyVersions/1"
                                                                                                             Private key path ($IFLX_PRO_LIC_PRIVATE_KEY)
      --public-key="gcloud-kms_global_clustered-licensing_signing-key_v1.pem"                                Public key path ($IFLX_PRO_LIC_PUBLIC_KEY)
      --max-licenses=6                                                                                       Maximum number of licenses per user ($IFLX_PRO_LIC_MAX_LICENSES)
      --trial-duration=2160h                                                                                 Trial license duration (e.g. 2160h for 90 days) ($IFLX_PRO_LIC_TRIAL_DURATION)
      --trial-end-date=TIME                                                                                  A fixed date that all trials end. Ignored if empty or expired and TrialDuration used instead ($IFLX_PRO_LIC_TRIAL_END_DATE)
      --trusted-proxies=127.0.0.1/32,...                                                                     Trusted proxy CIDR ranges (e.g., '10.0.0.0/8,172.16.0.0/12') ($IFLX_PRO_LIC_TRUSTED_PROXIES)
```

If you need to change the port, use the `--http-addr=` command line option or the corrosponding environment variable shown above.

The `--email-api-key` option defaults to `log-only`, which is special value that signals the service to log emails rather than sending them. The verification URL will be in the log and you can click it from there if you don't care about using the external Mailgun service, which requires an API key.

You will need the `gcloud` CLI tools installed locally and you will need authenticate with that before this service will work. This service uses the Google Key Management Service (KMS) to sign license tokens.

#### Test the service using curl

Create a new user and request a license:

```
curl -X POST "http://localhost:8687/licenses" \
     -d "email=david@influxdata.com" \
     -d "writer-id=influxdbpro1" \
     -d "instance-id=`uuidgen`"
```

You can also use an email address like `david+test1@influxdata.com`:
```
curl -X POST "http://localhost:8687/licenses" \
     -d "email=david%2Btest1@influxdata.com" \
     -d "writer-id=influxdbpro2" \
     -d "instance-id=`uuidgen`"
```

#### Creating or modifying project infrastucture
This section is about creating or changing resources within the Google Cloud
Project where this license servic is deployed. This is not necessary for normal
day-to-day builds and deployments of the service. This is only required
if you need to, for example, increase the number of CPUs or GBs of RAM
Postgress has, create new service accounts, modify the VPC, etc.

This infrastructure is maintained using Terraform and those and you'll need
it installed to make changes. When it doubt, please consult someone in SRE
or your manager before applying infrastruture changes to this project. It 
is customer-facing.

The terraform files are in the `influxdb3_licensing/terraform` directory.
Make the necessary changes there and then:
```
terraform apply
```
It will display its planned changes. Review them and they type 'yes' and hit
Enter to apply them.
## Chronograf TLS

Chronograf supports TLS to securely communicate between the browser and server via
HTTPS.

We recommend using HTTPS with Chronograf.  If you are not using a TLS termination proxy,
you can run Chronograf's server with TLS connections.
### TL;DR

```sh
chronograf --cert=my.crt --key=my.key
```

### Running Chronograf with TLS

Chronograf server has command line and environment variable options to specify
the certificate and key files.  The server reads and parses a public/private key
pair from these files. The files must contain PEM encoded data.

In Chronograf all command line options also have a corresponding environment
variable. 

To specify the certificate file either use the `--cert` CLI option or `TLS_CERTIFICATE`
environment variable.

To specify the key file either use the `--key` CLI option or `TLS_PRIVATE_KEY`
environment variable.

To specify the certificate and key if both are in the same file either use the `--cert`
CLI option or `TLS_CERTIFICATE` environment variable.

#### Example with CLI options
```sh
chronograf --cert=my.crt --key=my.key
```

#### Example with environment variables
```sh
TLS_CERTIFICATE=my.crt TLS_PRIVATE_KEY=my.key chronograf
```

#### Docker example with environment variables
```sh
docker run -v /host/path/to/certs:/certs -e TLS_CERTIFICATE=/certs/my.crt -e TLS_PRIVATE_KEY=/certs/my.key quay.io/influxdb/chronograf:latest
```

### Testing with self-signed certificates
In a production environment you should not use self-signed certificates.  However,
for testing it is fast to create your own certs.

To create a cert and key in one file with openssl:

```sh
openssl req -x509 -newkey rsa:4096 -sha256 -nodes -keyout testing.pem -out testing.pem -subj "/CN=localhost" -days 365
```

Next, set the environment variable `TLS_CERTIFICATE`:
```sh
export TLS_CERTIFICATE=$PWD/testing.pem
```

Run chronograf:

```sh
./chronograf
INFO[0000] Serving chronograf at https://[::]:8888       component=server
```

In the first log message you should see `https` rather than `http`.

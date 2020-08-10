# X509 Cert Input Plugin

This plugin provides information about X509 certificate accessible via local
file or network connection.


### Configuration

```toml
# Reads metrics from a SSL certificate
[[inputs.x509_cert]]
  ## List certificate sources
  sources = ["/etc/ssl/certs/ssl-cert-snakeoil.pem", "https://example.org:443"]

  ## Timeout for SSL connection
  # timeout = "5s"

  ## Pass a different name into the TLS request (Server Name Indication)
  ##   example: server_name = "myhost.example.org"
  # server_name = "myhost.example.org"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
```


### Metrics

- x509_cert
  - tags:
    - source - source of the certificate
    - organization
    - organizational_unit
    - country
    - province
    - locality
    - verification
    - serial_number
    - signature_algorithm
    - public_key_algorithm
    - issuer_common_name
    - issuer_serial_number
    - san
  - fields:
    - verification_code (int)
    - verification_error (string)
    - expiry (int, seconds)
    - age (int, seconds)
    - startdate (int, seconds)
    - enddate (int, seconds)


### Example output

```
x509_cert,common_name=ubuntu,source=/etc/ssl/certs/ssl-cert-snakeoil.pem,verification=valid age=7693222i,enddate=1871249033i,expiry=307666777i,startdate=1555889033i,verification_code=0i 1563582256000000000
x509_cert,common_name=www.example.org,country=US,locality=Los\ Angeles,organization=Internet\ Corporation\ for\ Assigned\ Names\ and\ Numbers,organizational_unit=Technology,province=California,source=https://example.org:443,verification=invalid age=20219055i,enddate=1606910400i,expiry=43328144i,startdate=1543363200i,verification_code=1i,verification_error="x509: certificate signed by unknown authority" 1563582256000000000
x509_cert,common_name=DigiCert\ SHA2\ Secure\ Server\ CA,country=US,organization=DigiCert\ Inc,source=https://example.org:443,verification=valid age=200838255i,enddate=1678276800i,expiry=114694544i,startdate=1362744000i,verification_code=0i 1563582256000000000
x509_cert,common_name=DigiCert\ Global\ Root\ CA,country=US,organization=DigiCert\ Inc,organizational_unit=www.digicert.com,source=https://example.org:443,verification=valid age=400465455i,enddate=1952035200i,expiry=388452944i,startdate=1163116800i,verification_code=0i 1563582256000000000
```

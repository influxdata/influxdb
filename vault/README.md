# Vault Secret Service
This package implements `platform.SecretService` using [vault](https://github.com/hashicorp/vault).

## Key layout
All secrets are stored in vault as key value pairs that can be found under
the key `/secret/data/:orgID`.

For example

```txt
/secret/data/031c8cbefe101000 ->
  github_api_key: foo
  some_other_key: bar
  a_secret: key
```

## Configuration

When a new secret service is instatiated with `vault.NewSecretService()` we read the
environment for the [standard vault environment variables](https://www.vaultproject.io/docs/commands/index.html#environment-variables).

It is expected that the vault provided is unsealed and that the `VAULT_TOKEN` has sufficient privileges to access the key space described above.

## Test/Dev

The vault secret service may be used by starting a vault server

```sh
vault server -dev
```

```sh
VAULT_ADDR='<vault address>' VAULT_TOKEN='<vault token>' influxd --secret-store vault
```

Once the vault and influxdb servers have been started and initialized, you may test the service by executing the following:

```sh
curl --request GET \
  --url http://localhost:8086/api/v2/orgs/<org id>/secrets \
  --header 'authorization: Token <authorization token>

# should return
#
#  {
#    "links": {
#      "org": "/api/v2/orgs/031c8cbefe101000",
#      "secrets": "/api/v2/orgs/031c8cbefe101000/secrets"
#    },
#    "secrets": []
#  }
```

```sh
curl --request PATCH \
  --url http://localhost:8086/api/v2/orgs/<org id>/secrets \
  --header 'authorization: Token <authorization token> \
  --header 'content-type: application/json' \
  --data '{
	"foo": "bar",
	"hello": "world"
}'

# should return 204 no content
```

```sh
curl --request GET \
  --url http://localhost:8086/api/v2/orgs/<org id>/secrets \
  --header 'authorization: Token <authorization token>

# should return
#
#  {
#    "links": {
#      "org": "/api/v2/orgs/031c8cbefe101000",
#      "secrets": "/api/v2/orgs/031c8cbefe101000/secrets"
#    },
#    "secrets": [
#      "foo",
#      "hello"
#    ]
#  }
```


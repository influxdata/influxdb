## Chronograf with OAuth 2.0 (Github-style)

OAuth 2.0 Style Authentication


### Configuration

To use authentication in Chronograf, both Github OAuth and JWT signature need to be configured.

#### Configuring JWT signature

Set a [JWT](https://tools.ietf.org/html/rfc7519) signature to a random string. This is needed for all OAuth2 providers that you choose to configure. *Keep this random string around!*

You'll need it each time you start a chronograf server because it is used to verify user authorization. If you are running multiple chronograf servers in an HA configuration set the `TOKEN_SECRET` on each to allow users to stay logged in.

```sh
export TOKEN_SECRET=supersupersecret
```

# Github

#### Creating Github OAuth Application

To create a Github OAuth Application follow the [Register your app](https://developer.github.com/guides/basics-of-authentication/#registering-your-app) instructions.
Essentially, you'll register your application [here](https://github.com/settings/applications/new)

The `Homepage URL` should be Chronograf's full server name and port.  If you are running it locally for example, make it `http://localhost:8888`

The `Authorization callback URL` must be the location of the `Homepage URL` plus `/oauth/github/callback`.  For example, if `Homepage URL` was 
`http://localhost:8888` then the `Authorization callback URL` should be `http://localhost:8888/oauth/github/callback`.

Github will provide a `Client ID` and `Client Secret`.  To register these values with chronograf set the following environment variables:

* `GH_CLIENT_ID` 
* `GH_CLIENT_SECRET` 

For example:

```sh
export GH_CLIENT_ID=b339dd4fddd95abec9aa
export GH_CLIENT_SECRET=260041897d3252c146ece6b46ba39bc1e54416dc
```

#### Optional Github Organizations

To require an organization membership for a user, set the `GH_ORGS` environment variables
```sh
export GH_ORGS=biffs-gang
```

If the user is not a member, then the user will not be allowed access.

To support multiple organizations use a comma delimted list like so:

```sh
export GH_ORGS=hill-valley-preservation-sociey,the-pinheads
```

# Google

#### Creating Google OAuth Application

You will need to obtain a client ID and an application secret by following the steps under "Basic Steps" [here](https://developers.google.com/identity/protocols/OAuth2). Chronograf will also need to be publicly accessible via a fully qualified domain name so that Google properly redirects users back to the application.

This information should be set in the following ENVs:

* `GOOGLE_CLIENT_ID`
* `GOOGLE_CLIENT_SECRET`
* `PUBLIC_URL`

Alternatively, this can also be set using the command line switches:

* `--google-client-id`
* `--google-client-secret`
* `--public-url`

#### Optional Google Domains

Similar to Github's organization restriction, Google authentication can be restricted to permit access to Chronograf from only specific domains. These are configured using the `GOOGLE_DOMAINS` ENV or the `--google-domains` switch. Multiple domains are separated with a comma. For example, if we wanted to permit access only from biffspleasurepalace.com and savetheclocktower.com the ENV would be set as follows:

```sh
export GOOGLE_DOMAINS=biffspleasurepalance.com,savetheclocktower.com
```

# Heroku

#### Creating Heroku Application

To obtain a client ID and application secret for Heroku, you will need to follow the guide posted [here](https://devcenter.heroku.com/articles/oauth#register-client). Once your application has been created, those two values should be inserted into the following ENVs:

* `HEROKU_CLIENT_ID`
* `HEROKU_SECRET`

The equivalent command line switches are:

* `--heroku-client-id`
* `--heroku-secret`

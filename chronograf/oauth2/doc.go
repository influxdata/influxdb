// Package oauth2 provides http.Handlers necessary for implementing Oauth2
// authentication with multiple Providers.
//
// This is how the pieces of this package fit together:
//
//  ┌────────────────────────────────────────┐
//  │github.com/influxdata/influxdb/chronograf/oauth2 │
//  ├────────────────────────────────────────┴────────────────────────────────────┐
//  │┌────────────────────┐                                                       │
//  ││   <<interface>>    │        ┌─────────────────────────┐                    │
//  ││   Authenticator    │        │         AuthMux         │                    │
//  │├────────────────────┤        ├─────────────────────────┤                    │
//  ││Authorize()         │   Auth │+SuccessURL : string     │                    │
//  ││Validate()          ◀────────│+FailureURL : string     │──────────┐         │
//  ||Expire()            |        |+Now : func() time.Time  |          |         |
//  │└──────────△─────────┘        └─────────────────────────┘          |         |
//  │           │                               │                       │         |
//  │           │                               │                       │         │
//  │           │                               │                       │         │
//  │           │                       Provider│                       │         │
//  │           │                           ┌───┘                       │         │
//  │┌──────────┴────────────┐              │                           ▽         │
//  ││         Tokenizer     │              │                   ┌───────────────┐ │
//  │├───────────────────────┤              ▼                   │ <<interface>> │ │
//  ││Create()               │      ┌───────────────┐           │   OAuth2Mux   │ │
//  ││ValidPrincipal()       │      │ <<interface>> │           ├───────────────┤ │
//  │└───────────────────────┘      │   Provider    │           │Login()        │ │
//  │                               ├───────────────┤           │Logout()       │ │
//  │                               │ID()           │           │Callback()     │ │
//  │                               │Scopes()       │           └───────────────┘ │
//  │                               │Secret()       │                             │
//  │                               │Authenticator()│                             │
//  │                               └───────────────┘                             │
//  │                                       △                                     │
//  │                                       │                                     │
//  │             ┌─────────────────────────┼─────────────────────────┐           │
//  │             │                         │                         │           │
//  │             │                         │                         │           │
//  │             │                         │                         │           │
//  │ ┌───────────────────────┐ ┌──────────────────────┐  ┌──────────────────────┐│
//  │ │        Github         │ │        Google        │  │        Heroku        ││
//  │ ├───────────────────────┤ ├──────────────────────┤  ├──────────────────────┤│
//  │ │+ClientID : string     │ │+ClientID : string    │  │+ClientID : string    ││
//  │ │+ClientSecret : string │ │+ClientSecret : string│  │+ClientSecret : string││
//  │ │+Orgs : []string       │ │+Domains : []string   │  └──────────────────────┘│
//  │ └───────────────────────┘ │+RedirectURL : string │                          │
//  │                           └──────────────────────┘                          │
//  └─────────────────────────────────────────────────────────────────────────────┘
//
// The design focuses on an Authenticator, a Provider, and an OAuth2Mux. Their
// responsibilities, respectively, are to decode and encode secrets received
// from a Provider, to perform Provider specific operations in order to extract
// information about a user, and to produce the handlers which persist secrets.
// To add a new provider, You need only implement the Provider interface, and
// add its endpoints to the server Mux.
//
// The Oauth2 flow between a browser, backend, and a Provider that this package
// implements is pictured below for reference.
//
//      ┌─────────┐                ┌───────────┐                     ┌────────┐
//      │ Browser │                │Chronograf │                     │Provider│
//      └─────────┘                └───────────┘                     └────────┘
//           │                           │                                │
//           ├─────── GET /auth ─────────▶                                │
//           │                           │                                │
//           │                           │                                │
//           ◀ ─ ─ ─302 to Provider  ─ ─ ┤                                │
//           │                           │                                │
//           │                           │                                │
//           ├──────────────── GET /auth w/ callback ─────────────────────▶
//           │                           │                                │
//           │                           │                                │
//           ◀─ ─ ─ ─ ─ ─ ─   302 to Chronograf Callback  ─ ─ ─ ─ ─ ─ ─ ─ ┤
//           │                           │                                │
//           │   Code and State from     │                                │
//           │        Provider           │                                │
//           ├───────────────────────────▶    Request token w/ code &     │
//           │                           │             state              │
//           │                           ├────────────────────────────────▶
//           │                           │                                │
//           │                           │          Response with         │
//           │                           │              Token             │
//           │   Set cookie, Redirect    │◀ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤
//           │           to /            │                                │
//           ◀───────────────────────────┤                                │
//           │                           │                                │
//           │                           │                                │
//           │                           │                                │
//           │                           │                                │
//
// The browser ultimately receives a cookie from Chronograf, authorizing it.
// Its contents are encoded as a JWT whose "sub" claim is the user's email
// address for whatever provider they have authenticated with. Each request to
// Chronograf will validate the contents of this JWT against the `TOKEN_SECRET`
// and checked for expiration. The JWT's "sub" becomes the
// https://en.wikipedia.org/wiki/Principal_(computer_security) used for
// authorization to resources.
//
// The Mux is responsible for providing three http.Handlers for servicing the
// above interaction. These are mounted at specific endpoints by convention
// shared with the front end. Any future Provider routes should follow the same
// convention to ensure compatibility with the front end logic. These routes
// and their responsibilities are:
//
//   /oauth/{provider}/login
//
// The `/oauth` endpoint redirects to the Provider for OAuth.  Chronograf sets
// the OAuth `state` request parameter to a JWT with a random "sub".  Using
// $TOKEN_SECRET `/oauth/github/callback` can validate the `state` parameter
// without needing `state` to be saved.
//
//   /oauth/{provider}/callback
//
// The `/oauth/github/callback` receives the OAuth `authorization code`  and `state`.
//
// First, it will validate the `state` JWT from the `/oauth` endpoint. `JWT` validation
// only requires access to the signature token.  Therefore, there is no need for `state`
// to be saved.  Additionally, multiple Chronograf servers will not need to share third
// party storage to synchronize `state`. If this validation fails, the request
// will be redirected to `/login`.
//
// Secondly, the endpoint will use the `authorization code` to retrieve a valid OAuth token
// with the `user:email` scope.  If unable to get a token from Github, the request will
// be redirected to `/login`.
//
// Finally, the endpoint will attempt to get the primary email address of the
// user.  Again, if not successful, the request will redirect to `/login`.
//
// The email address is used as the subject claim for a new JWT.  This JWT becomes the
// value of the cookie sent back to the browser. The cookie is valid for thirty days.
//
// Next, the request is redirected to `/`.
//
// For all API calls to `/chronograf/v1`, the server checks for the existence and validity
// of the JWT within the cookie value.
// If the request did not have a valid JWT, the API returns `HTTP/1.1 401 Unauthorized`.
//
//   /oauth/{provider}/logout
//
// Simply expires the session cookie and redirects to `/`.
package oauth2

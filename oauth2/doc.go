// The oauth2 package provides http.Handlers necessary for implementing Oauth2
// authentication with multiple Providers.
//
// This is how the pieces of this package fit together:
//
//  ┌────────────────────────────────────────┐
//  │github.com/influxdata/chronograf/oauth2 │
//  ├────────────────────────────────────────┴────────────────────────────────────┐
//  │┌────────────────────┐                                                       │
//  ││   <<interface>>    │        ┌─────────────────────────┐                    │
//  ││   Authenticator    │        │        CookieMux        │                    │
//  │├────────────────────┤        ├─────────────────────────┤                    │
//  ││Authenticate()      │   Auth │+SuccessURL : string     │                    │
//  ││Token()             ◀────────│+FailureURL : string     │──────────┐         │
//  │└──────────△─────────┘        │+Now : func() time.Time  │          │         │
//  │           │                  └─────────────────────────┘          │         │
//  │           │                               │                       │         │
//  │           │                               │                       │         │
//  │           │                       Provider│                       │         │
//  │           │                           ┌───┘                       │         │
//  │┌──────────┴────────────┐              │                           ▽         │
//  ││          JWT          │              │                   ┌───────────────┐ │
//  │├───────────────────────┤              ▼                   │ <<interface>> │ │
//  ││+Secret : string       │      ┌───────────────┐           │   OAuth2Mux   │ │
//  ││+Now : func() time.Time│      │ <<interface>> │           ├───────────────┤ │
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
// information about a user, and to provide the handlers which persist secrets.
// To add a new provider, You need only implement the Provider interface, and
// add its endpoints to the server.Mux.
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
package oauth2

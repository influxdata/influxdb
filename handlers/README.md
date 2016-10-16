### Handler Stack

Entry (JWT Check) ---> No JWT -> Login Page --> redirect to OAuth provider --> redirect to /token --> return JWT token --> back to Entry
                       |                 ^
                       |                 |
                       |                 |
                       |                bad
                       |                 |
                       v                 |
                       JWT --> Check token ---> good --> Return resource or asset


### Authorized
Once the user has been authenticated, the github email address is sent via the context.Context to
the follow-on requests.  The value is keyed with mrfusion.PrincipalKey

To get:

```go
    principal := ctxt.Value(mrfusion.PrincipalKey).(mrfusion.Principal)
```

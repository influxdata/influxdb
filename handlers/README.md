### Handler Stack

Entry (JWT Check) ---> No JWT -> Login Page --> redirect to OAuth provider --> redirect to /token --> return JWT token --> back to Entry
                       |                 ^
                       |                 |
                       |                 |
                       |                bad
                       |                 |
                       v                 |
                       JWT --> Check token ---> good --> Return resource or asset


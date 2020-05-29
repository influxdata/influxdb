# errors.go

This is inspired from Ben Johnson's blog post [Failure is Your Domain](https://middlemost.com/failure-is-your-domain/)

## The Error struct 

```go

    type Error struct {
        Code string
        Msg string
        Op string
        Err error
    }  
```

    * Code is the machine readable code, for reference purpose. All the codes should be a constant string. For example. `const ENotFound = "source not found"`.

    * Msg is the human readable message for end user. For example, `Your credit card is declined.`

    * Op is the logical Operator, should be a constant defined inside the function. For example: "bolt.UserCreate".

    * Err is the embed error. You may embed either a third party error or and platform.Error.

## Use Case Example

We inplement the following interface

```go

    type OrganizationService interface {
        FindOrganizationByID(ctx context.Context, id ID) (*Organization, error)
    }

    func (c *Client)FindOrganizationByID(ctx context.Context, id platform.ID) (*platform.Organization, error) {
        var o *platform.Organization
        const op = "bolt.FindOrganizationByID"
        err := c.db.View(func(tx *bolt.Tx) error {
            org, err := c.findOrganizationByID(ctx, tx, id)
            if err != nil {
                return err
            }
            o = org
            return nil
        })

        if err != nil {
            return nil, &platform.Error{
                Code: platform.ENotFound,
                Op: op,
                Err: err,
            }
        }
        return o, nil
    }
```

To check the error code

```go

    if platform.ErrorCode(err) == platform.ENotFound {
        ...
    }
```

To serialize the error

```go

    b, err := json.Marshal(err)
```

To deserialize the error

```go

    e := new(platform.Error)
    err := json.Unmarshal(b, e)
```







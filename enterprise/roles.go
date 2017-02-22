package enterprise

/*
// Add creates a new Role in Influx Enterprise
func (c *Client) Add(ctx context.Context, u *chronograf.Role) (*chronograf.Role, error) {
	if err := c.Ctrl.CreateRole(ctx, u.Name, u.Passwd); err != nil {
		return nil, err
	}
	return u, nil
}

// Delete the Role from Influx Enterprise
func (c *Client) Delete(ctx context.Context, u *chronograf.Role) error {
	return c.Ctrl.DeleteRole(ctx, u.Name)
}

// Get retrieves a Role if name exists.
func (c *Client) Get(ctx context.Context, name string) (*chronograf.Role, error) {
	u, err := c.Ctrl.Role(ctx, name)
	if err != nil {
		return nil, err
	}
	return &chronograf.Role{
		Name:        u.Name,
		Permissions: toChronograf(u.Permissions),
	}, nil
}

// Update the Role's permissions or roles
func (c *Client) Update(ctx context.Context, u *chronograf.Role) error {
	perms := toEnterprise(u.Permissions)
	return c.Ctrl.SetRolePerms(ctx, u.Name, perms)
}

// All is all Roles in influx
func (c *Client) All(ctx context.Context) ([]chronograf.Role, error) {
	all, err := c.Ctrl.Roles(ctx, nil)
	if err != nil {
		return nil, err
	}

	res := make([]chronograf.Role, len(all.Roles))
	for i, Role := range all.Roles {
		res[i] = chronograf.Role{
			Name:        Role.Name,
			Permissions: toChronograf(Role.Permissions),
		}
	}
	return res, nil
}*/

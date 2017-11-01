import reducer from 'src/admin/reducers/influxdb'

import {
  addUser,
  addRole,
  addDatabase,
  addRetentionPolicy,
  syncUser,
  syncRole,
  editUser,
  editRole,
  editDatabase,
  editRetentionPolicy,
  loadRoles,
  loadPermissions,
  deleteRole,
  deleteUser,
  removeDatabase,
  removeRetentionPolicy,
  filterRoles,
  filterUsers,
  addDatabaseDeleteCode,
  removeDatabaseDeleteCode,
} from 'src/admin/actions/influxdb'

import {
  NEW_DEFAULT_USER,
  NEW_DEFAULT_ROLE,
  NEW_DEFAULT_DATABASE,
  NEW_EMPTY_RP,
} from 'src/admin/constants'

let state

// Users
const u1 = {
  name: 'acidburn',
  roles: [
    {
      name: 'hax0r',
      permissions: {
        allowed: [
          'ViewAdmin',
          'ViewChronograf',
          'CreateDatabase',
          'CreateUserAndRole',
          'AddRemoveNode',
          'DropDatabase',
          'DropData',
          'ReadData',
          'WriteData',
          'Rebalance',
          'ManageShard',
          'ManageContinuousQuery',
          'ManageQuery',
          'ManageSubscription',
          'Monitor',
          'CopyShard',
          'KapacitorAPI',
          'KapacitorConfigAPI',
        ],
        scope: 'all',
      },
    },
  ],
  permissions: [],
  links: {self: '/chronograf/v1/sources/1/users/acidburn'},
}
const u2 = {
  name: 'zerocool',
  roles: [],
  permissions: [],
  links: {self: '/chronograf/v1/sources/1/users/zerocool'},
}
const users = [u1, u2]

// Roles
const r1 = {
  name: 'hax0r',
  users: [],
  permissions: [
    {
      allowed: [
        'ViewAdmin',
        'ViewChronograf',
        'CreateDatabase',
        'CreateUserAndRole',
        'AddRemoveNode',
        'DropDatabase',
        'DropData',
        'ReadData',
        'WriteData',
        'Rebalance',
        'ManageShard',
        'ManageContinuousQuery',
        'ManageQuery',
        'ManageSubscription',
        'Monitor',
        'CopyShard',
        'KapacitorAPI',
        'KapacitorConfigAPI',
      ],
      scope: 'all',
    },
  ],
  links: {self: '/chronograf/v1/sources/1/roles/hax0r'},
}
const r2 = {
  name: 'l33tus3r',
  links: {self: '/chronograf/v1/sources/1/roles/l33tus3r'},
}
const roles = [r1, r2]

// Permissions
const global = {scope: 'all', allowed: ['p1', 'p2']}
const scoped = {scope: 'db1', allowed: ['p1', 'p3']}
const permissions = [global, scoped]

// Databases && Retention Policies
const rp1 = {
  name: 'rp1',
  duration: '0',
  replication: 2,
  isDefault: true,
  links: {self: '/chronograf/v1/sources/1/db/db1/rp/rp1'},
}

const db1 = {
  name: 'db1',
  links: {self: '/chronograf/v1/sources/1/db/db1'},
  retentionPolicies: [rp1],
}

const db2 = {
  name: 'db2',
  links: {self: '/chronograf/v1/sources/1/db/db2'},
  retentionPolicies: [],
  deleteCode: 'DELETE',
}

describe('Admin.Reducers', () => {
  describe('Databases', () => {
    const state = {databases: [db1, db2]}

    it('can add a database', () => {
      const actual = reducer(state, addDatabase())
      const expected = [{...NEW_DEFAULT_DATABASE, isEditing: true}, db1, db2]
      expect(actual.databases.length).to.equal(expected.length)
      expect(actual.databases[0].name).to.equal(expected[0].name)
      expect(actual.databases[0].isNew).to.equal(expected[0].isNew)
      expect(actual.databases[0].retentionPolicies).to.equal(
        expected[0].retentionPolicies
      )
    })

    it('can edit a database', () => {
      const updates = {name: 'dbOne'}
      const actual = reducer(state, editDatabase(db1, updates))
      const expected = [{...db1, ...updates}, db2]

      expect(actual.databases).to.deep.equal(expected)
    })

    it('can remove a database', () => {
      const actual = reducer(state, removeDatabase(db1))
      const expected = [db2]

      expect(actual.databases).to.deep.equal(expected)
    })

    it('can add a database delete code', () => {
      const actual = reducer(state, addDatabaseDeleteCode(db1))
      const expected = [{...db1, deleteCode: ''}, db2]

      expect(actual.databases).to.deep.equal(expected)
    })

    it('can remove the delete code', () => {
      const actual = reducer(state, removeDatabaseDeleteCode(db2))
      delete db2.deleteCode
      const expected = [db1, db2]

      expect(actual.databases).to.deep.equal(expected)
    })
  })

  describe('Retention Policies', () => {
    const state = {databases: [db1]}

    it('can add a retention policy', () => {
      const actual = reducer(state, addRetentionPolicy(db1))
      const expected = [{...db1, retentionPolicies: [NEW_EMPTY_RP, rp1]}]

      expect(actual.databases).to.deep.equal(expected)
    })

    it('can remove a retention policy', () => {
      const actual = reducer(state, removeRetentionPolicy(db1, rp1))
      const expected = [{...db1, retentionPolicies: []}]

      expect(actual.databases).to.deep.equal(expected)
    })

    it('can edit a retention policy', () => {
      const updates = {name: 'rpOne', duration: '100y', replication: '42'}
      const actual = reducer(state, editRetentionPolicy(db1, rp1, updates))
      const expected = [{...db1, retentionPolicies: [{...rp1, ...updates}]}]

      expect(actual.databases).to.deep.equal(expected)
    })
  })

  it('it can add a user', () => {
    state = {
      users: [u1],
    }

    const actual = reducer(state, addUser())
    const expected = {
      users: [{...NEW_DEFAULT_USER, isEditing: true}, u1],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  it('it can sync a stale user', () => {
    const staleUser = {...u1, roles: []}
    state = {users: [u2, staleUser]}

    const actual = reducer(state, syncUser(staleUser, u1))
    const expected = {
      users: [u2, u1],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  it('it can edit a user', () => {
    const updates = {name: 'onecool'}
    state = {
      users: [u2, u1],
    }

    const actual = reducer(state, editUser(u2, updates))
    const expected = {
      users: [{...u2, ...updates}, u1],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  it('it can add a role', () => {
    state = {
      roles: [r1],
    }

    const actual = reducer(state, addRole())
    const expected = {
      roles: [{...NEW_DEFAULT_ROLE, isEditing: true}, r1],
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can sync a stale role', () => {
    const staleRole = {...r1, permissions: []}
    state = {roles: [r2, staleRole]}

    const actual = reducer(state, syncRole(staleRole, r1))
    const expected = {
      roles: [r2, r1],
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can edit a role', () => {
    const updates = {name: 'onecool'}
    state = {
      roles: [r2, r1],
    }

    const actual = reducer(state, editRole(r2, updates))
    const expected = {
      roles: [{...r2, ...updates}, r1],
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can load the roles', () => {
    const actual = reducer(state, loadRoles({roles}))
    const expected = {
      roles,
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can delete a role', () => {
    state = {
      roles: [r1],
    }

    const actual = reducer(state, deleteRole(r1))
    const expected = {
      roles: [],
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can delete a user', () => {
    state = {
      users: [u1],
    }

    const actual = reducer(state, deleteUser(u1))
    const expected = {
      users: [],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  it('can filter roles w/ "x" text', () => {
    state = {
      roles,
    }

    const text = 'x'

    const actual = reducer(state, filterRoles(text))
    const expected = {
      roles: [{...r1, hidden: false}, {...r2, hidden: true}],
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('can filter users w/ "zero" text', () => {
    state = {
      users,
    }

    const text = 'zero'

    const actual = reducer(state, filterUsers(text))
    const expected = {
      users: [{...u1, hidden: true}, {...u2, hidden: false}],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  // Permissions
  it('it can load the permissions', () => {
    const actual = reducer(state, loadPermissions({permissions}))
    const expected = {
      permissions,
    }

    expect(actual.permissions).to.deep.equal(expected.permissions)
  })
})

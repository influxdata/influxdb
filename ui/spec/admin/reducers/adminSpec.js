import reducer from 'src/admin/reducers/admin'

import {
  addUser,
  createUserSuccess,
  editUser,
  loadRoles,
  loadPermissions,
  deleteRole,
  deleteUser,
  filterRoles,
  filterUsers,
} from 'src/admin/actions'

let state = undefined

// Roles
const r1 = {name: 'role1', links: {self: '/chronograf/v1/sources/1/roles/role1'}}
const r2 = {name: 'role2', links: {self: '/chronograf/v1/sources/1/roles/role2'}}
const roles = [r1, r2]

// Users
const u1 = {
  name: 'acidburn',
  roles: [],
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
const newDefaultUser = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
  links: {self: ''},
  isNew: true,
}

// Permissions
const global = {scope: 'all', allowed: ['p1', 'p2']}
const scoped = {scope: 'db1', allowed: ['p1', 'p3']}
const permissions = [global, scoped]

describe('Admin.Reducers', () => {
  it('it can add a user', () => {
    state = {
      users: [
        u1,
      ]
    }

    const actual = reducer(state, addUser())
    const expected = {
      users: [
        {...newDefaultUser, isEditing: true},
        u1,
      ],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  it('it can confirm a created user', () => {
    const addedUser = {
      name: 'acidburn',
      password: 'pass1',
      roles: [],
      permissions: [],
      links: {self: ''},
      isNew: true,
    }
    state = {users: [u2, addedUser]}

    const actual = reducer(state, createUserSuccess(addedUser, u1))
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
      users: [{...u2, ...updates}, u1]
    }

    expect(actual.users).to.deep.equal(expected.users)
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
      roles: [
        r1,
      ]
    }

    const actual = reducer(state, deleteRole(r1))
    const expected = {
      roles: [],
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can delete a user', () => {
    state = {
      users: [
        u1,
      ]
    }

    const actual = reducer(state, deleteUser(u1))
    const expected = {
      users: [],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })

  it('can filter roles w/ "1" text', () => {
    state = {
      roles,
    }

    const text = '1'

    const actual = reducer(state, filterRoles(text))
    const expected = {
      roles: [
        {...r1, hidden: false},
        {...r2, hidden: true},
      ],
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
      users: [
        {...u1, hidden: true},
        {...u2, hidden: false},
      ],
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

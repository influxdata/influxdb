import reducer from 'src/admin/reducers/admin'

import {
  loadRoles,
  deleteRole,
  deleteUser,
} from 'src/admin/actions'

let state = undefined
const r1 = {name: 'role1'}
const r2 = {name: 'role2'}
const roles = [r1, r2]

const u1 = {name: 'user1'}
const u2 = {name: 'user2'}
const users = [u1, u2]

describe('Admin.Reducers', () => {
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
})

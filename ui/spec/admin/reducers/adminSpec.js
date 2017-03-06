import reducer from 'src/admin/reducers/admin'

import {
  loadRoles,
  deleteRole,
  deleteUser,
  filterRoles,
  filterUsers,
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

  it('can filter users w/ "2" text', () => {
    state = {
      users,
    }

    const text = '2'

    const actual = reducer(state, filterUsers(text))
    const expected = {
      users: [
        {...u1, hidden: true},
        {...u2, hidden: false},
      ],
    }

    expect(actual.users).to.deep.equal(expected.users)
  })
})

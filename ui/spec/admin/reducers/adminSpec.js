import reducer from 'src/admin/reducers/admin'

import {
  loadRoles,
  deleteRole,
} from 'src/admin/actions'

let state = undefined
const r1 = {name: 'role1'}
const r2 = {name: 'role2'}
const roles = [r1, r2]

describe('Admin.Reducers', () => {
  it('it can load the roles', () => {
    const actual = reducer(state, loadRoles({roles}))
    const expected = {
      roles,
    }

    expect(actual.roles).to.deep.equal(expected.roles)
  })

  it('it can delete the roles', () => {
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
})

import reducer from 'src/admin/reducers/chronograf'

import {loadUsers} from 'src/admin/actions/chronograf'

let state

const users = [
  {
    id: 666,
    name: 'bob@billietta.com',
    provider: 'GitHub',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'admin'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
    links: {self: '/chronograf/v1/users/666'},
  },
  {
    id: 667,
    name: 'billybob@gmail.com',
    provider: 'Auth0',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'viewer'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
    links: {self: '/chronograf/v1/users/667'},
  },
  {
    id: 720,
    name: 'shorty@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'editor'},
    ],
    links: {self: '/chronograf/v1/users/720'},
  },
  {
    id: 271,
    name: 'shawn.ofthe.dead@gmail.com',
    provider: 'GitHub',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
    links: {self: '/chronograf/v1/users/271'},
  },
  {
    id: 6389,
    name: 'swogglez@gmail.com',
    provider: 'Heroku',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Red Team', organizationID: 1236, name: 'viewer'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'viewer'},
    ],
    links: {self: '/chronograf/v1/users/6389'},
  },
  {
    id: 99181,
    name: 'whiskey.elbow@gmail.com',
    provider: 'GitHub',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'viewer'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'viewer'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'viewer'},
    ],
    links: {self: '/chronograf/v1/users/99181'},
  },
  {
    id: 3786,
    name: 'bob.builder@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
    links: {self: '/chronograf/v1/users/3786'},
  },
  {
    id: 112345,
    name: 'lost.in.translation@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
    ],
    links: {self: '/chronograf/v1/users/112345'},
  },
  {
    id: 23,
    name: 'wandering.soul@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
    ],
    links: {self: '/chronograf/v1/users/23'},
  },
  {
    id: 7,
    name: 'disembodied@gmail.com',
    provider: 'Auth0',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
    ],
    links: {self: '/chronograf/v1/users/7'},
  },
  {
    id: 0,
    name: 'bob.builder@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
    links: {self: '/chronograf/v1/users/0'},
  },
  {
    id: 2891,
    name: 'swag.bandit@gmail.com',
    provider: 'Google',
    scheme: 'OAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Blue Team', organizationID: 1234, name: 'admin'},
    ],
    links: {self: '/chronograf/v1/users/2891'},
  },
  {
    id: 2645,
    name: 'lord.ofthe.dance@gmail.com',
    provider: 'GitHub',
    scheme: 'OAuth2',
    superadmin: true,
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
    ],
    links: {self: '/chronograf/v1/users/2645'},
  },
  {
    id: 47119,
    name: 'ohnooeezzz@gmail.com',
    provider: 'Google',
    scheme: 'OAuth2',
    superadmin: true,
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: MEMBER_ROLE},
      {organizationName: 'Blue Team', organizationID: 1234, name: MEMBER_ROLE},
    ],
    links: {self: '/chronograf/v1/users/47119'},
  },
]

describe('Admin.Chronograf.Reducers', () => {
  it('it can load all users', () => {
    const actual = reducer(state, loadUsers({users}))
    const expected = {
      users,
    }

    expect(actual.users).to.deep.equal(expected.users)
  })
})

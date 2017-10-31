export const DUMMY_USERS = [
  {
    name: 'thealexpaxton@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Green Team', organizationID: 1234, name: 'admin'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
  },
  {
    name: 'billybob@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Green Team', organizationID: 1234, name: 'viewer'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'shorty@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Green Team', organizationID: 1234, name: 'editor'},
    ],
  },
  {
    name: 'shawn.ofthe.dead@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
  },
  {
    name: 'swogglez@gmail.com',
    provider: 'Heroku',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'viewer'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'viewer'},
    ],
  },
  {
    name: 'whiskey.elbow@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Green Team', organizationID: 1234, name: 'viewer'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'viewer'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'viewer'},
    ],
  },
  {
    name: 'bob.builder@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'lost.in.translation@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [{organizationName: 'All Users', organizationID: 666, name: 'none'}],
  },
  {
    name: 'wandering.soul@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [{organizationName: 'All Users', organizationID: 666, name: 'none'}],
  },
  {
    name: 'disembodied@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [{organizationName: 'All Users', organizationID: 666, name: 'none'}],
  },
  {
    name: 'bob.builder@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'swag.bandit@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Blue Team', organizationID: 1234, name: 'admin'},
    ],
  },
  {
    name: 'lord.ofthe.dance@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    superadmin: true,
    roles: [{organizationName: 'All Users', organizationID: 666, name: 'none'}],
  },
  {
    name: 'ohnooeezzz@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    superadmin: true,
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: 'none'},
      {organizationName: 'Blue Team', organizationID: 1234, name: 'none'},
    ],
  },
]

export const USER_ROLES = [
  {name: 'none'},
  {name: 'viewer'},
  {name: 'editor'},
  {name: 'admin'},
]
export const DEFAULT_ORG = 'All Users'
export const NO_ORG = 'None'

export const DUMMY_ORGS = [
  {name: DEFAULT_ORG},
  {name: NO_ORG},
  {name: 'Red Team'},
  {name: 'Blue Team'},
  {name: 'Green Team'},
]

export const NO_ROLE = 'No Role'

export const DUMMY_USERS = [
  {
    name: 'thealexpaxton@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'admin'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
  },
  {
    name: 'billybob@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'viewer'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'shorty@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Green Team', organizationID: 1234, name: 'editor'},
    ],
  },
  {
    name: 'shawn.ofthe.dead@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
  },
  {
    name: 'swogglez@gmail.com',
    provider: 'Heroku',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Red Team', organizationID: 1236, name: 'viewer'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'viewer'},
    ],
  },
  {
    name: 'whiskey.elbow@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
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
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'lost.in.translation@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
    ],
  },
  {
    name: 'wandering.soul@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
    ],
  },
  {
    name: 'disembodied@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
    ],
  },
  {
    name: 'bob.builder@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'swag.bandit@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Blue Team', organizationID: 1234, name: 'admin'},
    ],
  },
  {
    name: 'lord.ofthe.dance@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    superadmin: true,
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
    ],
  },
  {
    name: 'ohnooeezzz@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    superadmin: true,
    roles: [
      {organizationName: 'All Users', organizationID: 666, name: NO_ROLE},
      {organizationName: 'Blue Team', organizationID: 1234, name: NO_ROLE},
    ],
  },
]

export const USER_ROLES = [
  {name: NO_ROLE},
  {name: 'viewer'},
  {name: 'editor'},
  {name: 'admin'},
]
export const DEFAULT_ORG = 'All Users'
export const NO_ORG = 'No Org'

export const DUMMY_ORGS = [
  {name: DEFAULT_ORG},
  {name: NO_ORG},
  {name: 'Red Team'},
  {name: 'Blue Team'},
  {name: 'Green Team'},
]

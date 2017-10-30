export const DUMMY_USERS = [
  {
    name: 'thealexpaxton@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'Green Team', organizationID: 1234, name: 'admin'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
  },
  {
    name: 'billybob@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'Green Team', organizationID: 1234, name: 'viewer'},
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'shorty@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'Green Team', organizationID: 1234, name: 'editor'},
    ],
  },
  {
    name: 'shawn.ofthe.dead@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'Blue Team', organizationID: 1235, name: 'editor'},
    ],
  },
  {
    name: 'swogglez@gmail.com',
    provider: 'Heroku',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'Red Team', organizationID: 1236, name: 'viewer'},
      {organizationName: 'Blue Team', organizationID: 1235, name: 'viewer'},
    ],
  },
  {
    name: 'whiskey.elbow@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
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
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'lost.in.translation@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [],
  },
  {
    name: 'wandering.soul@gmail.com',
    provider: 'Heroku',
    scheme: 'LDAP',
    roles: [],
  },
  {
    name: 'disembodied@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [],
  },
  {
    name: 'bob.builder@gmail.com',
    provider: 'Generic',
    scheme: 'LDAP',
    roles: [
      {organizationName: 'Red Team', organizationID: 1236, name: 'editor'},
    ],
  },
  {
    name: 'swag.bandit@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    roles: [
      {organizationName: 'Blue Team', organizationID: 1234, name: 'admin'},
    ],
  },
  {
    name: 'lord.ofthe.dance@gmail.com',
    provider: 'GitHub',
    scheme: 'oAuth2',
    superadmin: true,
    roles: [],
  },
]

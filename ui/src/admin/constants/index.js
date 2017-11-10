export const TIMES = [
  {test: /ns/, magnitude: 0},
  {test: /µs/, magnitude: 1},
  {test: /u/, magnitude: 1},
  {test: /^\d*ms/, magnitude: 2},
  {test: /^\d*s/, magnitude: 3},
  {test: /^\d*m\d*s/, magnitude: 4},
  {test: /^\d*h\d*m\d*s/, magnitude: 5},
]

export const NEW_DEFAULT_USER = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
  links: {self: ''},
  isNew: true,
}

export const NEW_DEFAULT_ROLE = {
  name: '',
  permissions: [],
  users: [],
  links: {self: ''},
  isNew: true,
}

export const NEW_DEFAULT_RP = {
  name: 'autogen',
  duration: '0',
  replication: 2,
  isDefault: true,
  links: {self: ''},
}

export const NEW_EMPTY_RP = {
  name: '',
  duration: '',
  replication: 0,
  links: {self: ''},
  isNew: true,
}

export const NEW_DEFAULT_DATABASE = {
  name: '',
  isNew: true,
  retentionPolicies: [NEW_DEFAULT_RP],
}

export const WHITELIST_TOOLTIP =
  'If set to <code>true</code> users cannot<br/>authenticate unless an <strong>Admin</strong> adds<br/>them to the organization manually'

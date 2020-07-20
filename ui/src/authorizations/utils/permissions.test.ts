import {allAccessPermissions} from './permissions'
import {Permission} from 'src/types'

const hvhs: Permission[] = [
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'authorizations',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'authorizations',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'buckets',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'buckets',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'checks',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'checks',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'dashboards',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'dashboards',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'dbrp',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'dbrp',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'documents',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'documents',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'labels',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'labels',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'notificationRules',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'notificationRules',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'notificationEndpoints',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'notificationEndpoints',
    },
  },
  {
    action: 'read',
    resource: {
      id: 'bulldogs',
      type: 'orgs',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'secrets',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'secrets',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'scrapers',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'scrapers',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'sources',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'sources',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'stack',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'stack',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'tasks',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'tasks',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'telegrafs',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'telegrafs',
    },
  },
  {
    action: 'read',
    resource: {
      id: 'mario',
      type: 'users',
    },
  },
  {
    action: 'write',
    resource: {
      id: 'mario',
      type: 'users',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'variables',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'variables',
    },
  },
  {
    action: 'read',
    resource: {
      orgID: 'bulldogs',
      type: 'views',
    },
  },
  {
    action: 'write',
    resource: {
      orgID: 'bulldogs',
      type: 'views',
    },
  },
]

test('all-access tokens/authorizations production test', () => {
  expect(allAccessPermissions('bulldogs', 'mario')).toMatchObject(hvhs)
})

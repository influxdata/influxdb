import {Bucket, Permission, Authorization} from 'src/types'

export const allAccessPermissions = (orgID: string): Permission[] => [
  {
    action: 'read',
    resource: {type: 'orgs', id: orgID},
  },
  {
    action: 'read',
    resource: {type: 'authorizations', orgID},
  },
  {
    action: 'write',
    resource: {type: 'authorizations', orgID},
  },
  {
    action: 'read',
    resource: {type: 'buckets', orgID},
  },
  {
    action: 'write',
    resource: {type: 'buckets', orgID},
  },
  {
    action: 'read',
    resource: {type: 'dashboards', orgID},
  },
  {
    action: 'write',
    resource: {type: 'dashboards', orgID},
  },
  {
    action: 'read',
    resource: {type: 'sources', orgID},
  },
  {
    action: 'write',
    resource: {type: 'sources', orgID},
  },
  {
    action: 'read',
    resource: {type: 'tasks', orgID},
  },
  {
    action: 'write',
    resource: {type: 'tasks', orgID},
  },
  {
    action: 'read',
    resource: {type: 'telegrafs', orgID},
  },
  {
    action: 'write',
    resource: {type: 'telegrafs', orgID},
  },
  {
    action: 'read',
    resource: {type: 'users', orgID},
  },
  {
    action: 'write',
    resource: {type: 'users', orgID},
  },
  {
    action: 'read',
    resource: {type: 'variables', orgID},
  },
  {
    action: 'write',
    resource: {type: 'variables', orgID},
  },
  {
    action: 'read',
    resource: {type: 'scrapers', orgID},
  },
  {
    action: 'write',
    resource: {type: 'scrapers', orgID},
  },
  {
    action: 'read',
    resource: {type: 'secrets', orgID},
  },
  {
    action: 'write',
    resource: {type: 'secrets', orgID},
  },
  {
    action: 'read',
    resource: {type: 'labels', orgID},
  },
  {
    action: 'write',
    resource: {type: 'labels', orgID},
  },
  {
    action: 'read',
    resource: {type: 'views', orgID},
  },
  {
    action: 'write',
    resource: {type: 'views', orgID},
  },
  {
    action: 'read',
    resource: {type: 'documents', orgID},
  },
  {
    action: 'write',
    resource: {type: 'documents', orgID},
  },
]

export const specificBucketsPermissions = (
  buckets: Bucket[],
  permission: Permission['action']
): Permission[] => {
  return buckets.map(b => {
    return {
      action: permission,
      resource: {
        type: 'buckets' as 'buckets',
        orgID: b.orgID,
        id: b.id,
      },
    }
  })
}

export const allBucketsPermissions = (
  orgID: string,
  permission: Permission['action']
): Permission[] => {
  return [
    {
      action: permission,
      resource: {type: 'buckets', orgID},
    },
  ]
}

export const selectBucket = (
  bucketName: string,
  selectedBuckets: string[]
): string[] => {
  const isSelected = selectedBuckets.find(n => n === bucketName)

  if (isSelected) {
    return selectedBuckets.filter(n => n !== bucketName)
  }

  return [...selectedBuckets, bucketName]
}

export enum BucketTab {
  AllBuckets = 'All Buckets',
  Scoped = 'Scoped',
}

/*
  Given a list of authorizations, return only those that allow performing the
  supplied `action` to all of the supplied `bucketNames`.
*/
export const filterIrrelevantAuths = (
  auths: Authorization[],
  action: 'read' | 'write',
  bucketNames: string[]
): Authorization[] => {
  return auths.filter(auth =>
    bucketNames.every(bucketName =>
      auth.permissions.some(
        permission =>
          permission.action === action &&
          permission.resource.type === 'buckets' &&
          (!permission.resource.name || permission.resource.name === bucketName)
      )
    )
  )
}

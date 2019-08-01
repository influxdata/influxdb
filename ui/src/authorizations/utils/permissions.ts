import {Permission, PermissionResource, Authorization} from '@influxdata/influx'
import {Bucket} from 'src/types'

export const allAccessPermissions = (orgID: string) => [
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Orgs, id: orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Authorizations, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Authorizations, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Buckets, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Buckets, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Dashboards, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Dashboards, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Sources, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Sources, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Tasks, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Tasks, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Telegrafs, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Telegrafs, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Users, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Users, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Variables, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Variables, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Scrapers, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Scrapers, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Secrets, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Secrets, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Labels, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Labels, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Views, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Views, orgID},
  },
  {
    action: Permission.ActionEnum.Read,
    resource: {type: PermissionResource.TypeEnum.Documents, orgID},
  },
  {
    action: Permission.ActionEnum.Write,
    resource: {type: PermissionResource.TypeEnum.Documents, orgID},
  },
]

export const specificBucketsPermissions = (
  buckets: Bucket[],
  permission: Permission.ActionEnum
): Permission[] => {
  return buckets.map(b => {
    return {
      action: permission,
      resource: {
        type: PermissionResource.TypeEnum.Buckets,
        orgID: b.orgID,
        id: b.id,
      },
    }
  })
}

export const allBucketsPermissions = (
  orgID: string,
  permission: Permission.ActionEnum
): Permission[] => {
  return [
    {
      action: permission,
      resource: {type: PermissionResource.TypeEnum.Buckets, orgID},
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

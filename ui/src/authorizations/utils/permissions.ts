import {Permission, PermissionResource, Bucket} from '@influxdata/influx'

// Types

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

export const bucketPermissions = (
  orgID: string,
  permission: Permission.ActionEnum,
  buckets: Bucket[]
): Permission[] => {
  if (!buckets) {
    return allBucketsPermissions(orgID, permission)
  }

  return specificBucketsPermissions(buckets, permission)
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

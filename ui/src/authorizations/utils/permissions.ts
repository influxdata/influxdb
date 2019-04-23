import {Permission, PermissionResource} from '@influxdata/influx'

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

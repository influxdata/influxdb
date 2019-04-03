import {
  Permission,
  PermissionResource,
  Authorization,
  AuthorizationUpdateRequest,
} from '@influxdata/influx'

const {TypeEnum} = PermissionResource
const {ActionEnum} = Permission
const {StatusEnum} = AuthorizationUpdateRequest

export const authorization: Authorization = {
  links: {
    self: '/api/v2/authorizations/030444b11fb10000',
    user: '/api/v2/users/030444b10a710000',
  },
  id: '030444b11fb10000',
  token:
    'ohEmfY80A9UsW_cicNXgOMIPIsUvU6K9YcpTfCPQE3NV8Y6nTsCwVghczATBPyQh96CoZkOW5DIKldya6Y84KA==',
  status: StatusEnum.Active,
  user: 'watts',
  userID: '030444b10a710000',
  orgID: '030444b10a713000',
  description: 'im a token',
  permissions: [
    {action: ActionEnum.Write, resource: {type: TypeEnum.Orgs}},
    {action: ActionEnum.Write, resource: {type: TypeEnum.Buckets}},
  ],
}

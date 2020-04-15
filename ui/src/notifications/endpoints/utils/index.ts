import {NotificationEndpoint, PostNotificationEndpoint} from 'src/types'

export const toPostNotificationEndpoint = (
  endpoint: NotificationEndpoint
): PostNotificationEndpoint => {
  return {
    ...endpoint,
    status: endpoint.activeStatus,
  }
}

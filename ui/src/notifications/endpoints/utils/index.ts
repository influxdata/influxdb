import {NotificationEndpoint, PostNotificationEndpoint} from 'src/types'

export const toPostNotificationEndpoint = (
  endpoint: NotificationEndpoint
): PostNotificationEndpoint => {
  const labels = endpoint.labels || []

  return {
    ...endpoint,
    status: endpoint.endpointStatus,
    labels: labels.map(l => l.id),
  }
}

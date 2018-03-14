export const notify = notification => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})

export const dismissNotification = id => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})

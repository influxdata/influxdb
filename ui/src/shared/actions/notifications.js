export function publishNotification(type, message) {
  return {
    type: 'NOTIFICATION_RECEIVED',
    payload: {
      type,
      message,
    },
  };
}


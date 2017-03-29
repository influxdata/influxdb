export function receiveAuth(auth) {
  return {
    type: 'AUTH_RECEIVED',
    payload: {
      auth,
    },
  }
}

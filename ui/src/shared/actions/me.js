export function receiveMe(me) {
  return {
    type: 'ME_RECEIVED',
    payload: {
      me,
    },
  }
}

export function logout() {
  return {
    type: 'LOGOUT',
  }
}

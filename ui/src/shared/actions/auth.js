export const authExpired = auth => ({
  type: 'AUTH_EXPIRED',
  payload: {
    auth,
  },
})

export const authRequested = () => ({
  type: 'AUTH_REQUESTED',
})

export const authReceived = auth => ({
  type: 'AUTH_RECEIVED',
  payload: {
    auth,
  },
})

export const meRequested = () => ({
  type: 'ME_REQUESTED',
})

export const meReceived = me => ({
  type: 'ME_RECEIVED',
  payload: {
    me,
  },
})

export const logoutLinkReceived = logoutLink => ({
  type: 'LOGOUT_LINK_RECEIVED',
  payload: {
    logoutLink,
  },
})

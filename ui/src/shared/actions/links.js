import * as actionTypes from 'shared/constants/actionTypes'

export const linksReceived = links => ({
  type: actionTypes.LINKS_RECEIVED,
  payload: {links},
})

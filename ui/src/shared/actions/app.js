import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

// ephemeral state reducers
export const enablePresentationMode = () => ({
  type: 'ENABLE_PRESENTATION_MODE',
})

export const disablePresentationMode = () => ({
  type: 'DISABLE_PRESENTATION_MODE',
})

export const delayEnablePresentationMode = () => (dispatch) => {
  setTimeout(() => dispatch(enablePresentationMode()), PRESENTATION_MODE_ANIMATION_DELAY)
}

// persistent state reducers
export const setAutoRefresh = (milliseconds) => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})

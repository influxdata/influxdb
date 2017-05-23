import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

// ephemeral state action creators
export const enablePresentationMode = () => ({
  type: 'ENABLE_PRESENTATION_MODE',
})

export const disablePresentationMode = () => ({
  type: 'DISABLE_PRESENTATION_MODE',
})

export const delayEnablePresentationMode = () => dispatch => {
  setTimeout(
    () => dispatch(enablePresentationMode()),
    PRESENTATION_MODE_ANIMATION_DELAY
  )
}

// persistent state action creators
export const setAutoRefresh = milliseconds => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})

export const templateControlBarVisibilityToggled = () => ({
  type: 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED',
})

export const noop = () => ({
  type: 'NOOP',
  payload: {},
})

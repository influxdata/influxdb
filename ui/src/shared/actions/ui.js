import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

export function enablePresentationMode() {
  return {
    type: 'ENABLE_PRESENTATION_MODE',
  }
}

export function disablePresentationMode() {
  return {
    type: 'DISABLE_PRESENTATION_MODE',
  }
}

export function delayEnablePresentationMode() {
  return (dispatch) => {
    setTimeout(() => dispatch(enablePresentationMode()), PRESENTATION_MODE_ANIMATION_DELAY)
  }
}

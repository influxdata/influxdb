import {Dispatch} from 'redux'

import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

// ephemeral state action creators
interface EnablePresentationModeAction {
  type: 'ENABLE_PRESENTATION_MODE'
}
export const enablePresentationMode = (): EnablePresentationModeAction => ({
  type: 'ENABLE_PRESENTATION_MODE',
})

interface DisablePresentationModeAction {
  type: 'DISABLE_PRESENTATION_MODE'
}
export const disablePresentationMode = (): DisablePresentationModeAction => ({
  type: 'DISABLE_PRESENTATION_MODE',
})

export const delayEnablePresentationMode = (): ((
  dispatch: Dispatch<EnablePresentationModeAction>
) => void) => (dispatch: Dispatch<EnablePresentationModeAction>): void => {
  setTimeout(
    () => dispatch(enablePresentationMode()),
    PRESENTATION_MODE_ANIMATION_DELAY
  )
}

// persistent state action creators
interface SetAutoRefreshAction {
  type: 'SET_AUTOREFRESH'
  payload: {
    milliseconds: number
  }
}
export const setAutoRefresh = (milliseconds: number): SetAutoRefreshAction => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})

interface TemplateControlBarVisibilityToggledAction {
  type: 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED'
}
export const templateControlBarVisibilityToggled = (): TemplateControlBarVisibilityToggledAction => ({
  type: 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED',
})

interface NoopAction {
  type: 'NOOP'
  payload: object
}
export const noop = (): NoopAction => ({
  type: 'NOOP',
  payload: {},
})

import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {notify} from 'src/shared/actions/notifications'
import {notifyPresentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'

// ephemeral state action creators
export type EnablePresentationModeActionCreator = () => EnablePresentationModeAction

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

export type DelayEnablePresentationModeThunk = (
  dispatch: Dispatch<EnablePresentationModeAction>
) => Promise<NodeJS.Timer>

export const delayEnablePresentationMode: DelayEnablePresentationModeThunk = async (
  dispatch: Dispatch<EnablePresentationModeAction>
): Promise<NodeJS.Timer> =>
  setTimeout(() => {
    dispatch(enablePresentationMode())
    notify(notifyPresentationMode())
  }, PRESENTATION_MODE_ANIMATION_DELAY)

// persistent state action creators
export type SetAutoRefreshActionCreator = (
  milliseconds: number
) => SetAutoRefreshAction

interface SetAutoRefreshAction {
  type: 'SET_AUTOREFRESH'
  payload: {
    milliseconds: number
  }
}
export const setAutoRefresh: SetAutoRefreshActionCreator = (
  milliseconds: number
): SetAutoRefreshAction => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})

export type TemplateControlBarVisibilityToggledActionCreator = () => TemplateControlBarVisibilityToggledAction

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

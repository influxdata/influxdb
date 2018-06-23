import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {notify} from 'src/shared/actions/notifications'
import {notifyPresentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'
import * as Types from 'src/types/modules'

// ephemeral state action creators

export const enablePresentationMode = (): Types.App.Actions.EnablePresentationModeAction => ({
  type: 'ENABLE_PRESENTATION_MODE',
})

export const disablePresentationMode = (): Types.App.Actions.DisablePresentationModeAction => ({
  type: 'DISABLE_PRESENTATION_MODE',
})

export const delayEnablePresentationMode: Types.App.Actions.DelayEnablePresentationModeDispatcher = () => async (
  dispatch: Dispatch<Types.App.Actions.EnablePresentationModeAction>
): Promise<NodeJS.Timer> =>
  setTimeout(() => {
    dispatch(enablePresentationMode())
    notify(notifyPresentationMode())
  }, PRESENTATION_MODE_ANIMATION_DELAY)

// persistent state action creators

export const setAutoRefresh: Types.App.Actions.SetAutoRefreshActionCreator = (
  milliseconds: number
): Types.App.Actions.SetAutoRefreshAction => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})

export const templateControlBarVisibilityToggled = (): Types.App.Actions.TemplateControlBarVisibilityToggledAction => ({
  type: 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED',
})

export const noop = (): Types.App.Actions.NoopAction => ({
  type: 'NOOP',
  payload: {},
})

import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {notify} from 'src/shared/actions/notifications'
import {notifyPresentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'
import * as AppActions from 'src/types/actions/app'

// ephemeral state action creators

export const enablePresentationMode = (): AppActions.EnablePresentationModeAction => ({
  type: 'ENABLE_PRESENTATION_MODE',
})

export const disablePresentationMode = (): AppActions.DisablePresentationModeAction => ({
  type: 'DISABLE_PRESENTATION_MODE',
})

export const delayEnablePresentationMode: AppActions.DelayEnablePresentationModeDispatcher = () => async (
  dispatch: Dispatch<AppActions.EnablePresentationModeAction>
): Promise<NodeJS.Timer> =>
  setTimeout(() => {
    dispatch(enablePresentationMode())
    notify(notifyPresentationMode())
  }, PRESENTATION_MODE_ANIMATION_DELAY)

// persistent state action creators

export const setAutoRefresh: AppActions.SetAutoRefreshActionCreator = (
  milliseconds: number
): AppActions.SetAutoRefreshAction => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})

export const templateControlBarVisibilityToggled = (): AppActions.TemplateControlBarVisibilityToggledAction => ({
  type: 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED',
})

export const noop = (): AppActions.NoopAction => ({
  type: 'NOOP',
  payload: {},
})

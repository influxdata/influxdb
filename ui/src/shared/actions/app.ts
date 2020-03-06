import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {notify} from 'src/shared/actions/notifications'
import {presentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'
import {TimeZone} from 'src/types'

import {
  ActionTypes,
  EnablePresentationModeAction,
  DisablePresentationModeAction,
  EnableDashboardLightModeAction,
  DisableDashboardLightModeAction,
  DelayEnablePresentationModeDispatcher,
  SetAutoRefreshActionCreator,
  SetAutoRefreshAction,
  SetTimeZoneAction,
  TemplateControlBarVisibilityToggledAction,
} from 'src/types/actions/app'

// ephemeral state action creators

export const enablePresentationMode = (): EnablePresentationModeAction => ({
  type: ActionTypes.EnablePresentationMode,
})

export const disablePresentationMode = (): DisablePresentationModeAction => ({
  type: ActionTypes.DisablePresentationMode,
})

export const delayEnablePresentationMode: DelayEnablePresentationModeDispatcher = () => (
  dispatch: Dispatch<EnablePresentationModeAction>
): NodeJS.Timer =>
  setTimeout(() => {
    dispatch(enablePresentationMode())
    notify(presentationMode())
  }, PRESENTATION_MODE_ANIMATION_DELAY)

// persistent state action creators

export const setAutoRefresh: SetAutoRefreshActionCreator = (
  milliseconds: number
): SetAutoRefreshAction => ({
  type: ActionTypes.SetAutoRefresh,
  payload: {
    milliseconds,
  },
})

export const setTimeZone = (timeZone: TimeZone): SetTimeZoneAction => ({
  type: ActionTypes.SetTimeZone,
  payload: {timeZone},
})

export const templateControlBarVisibilityToggled = (): TemplateControlBarVisibilityToggledAction => ({
  type: ActionTypes.TemplateControlBarVisibilityToggled,
})

export const enableDashboardLightMode = (): EnableDashboardLightModeAction => ({
  type: ActionTypes.EnableDashboardLightMode,
})

export const disableDashboardLightMode = (): DisableDashboardLightModeAction => ({
  type: ActionTypes.DisableDashboardLightMode,
})

import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {notify} from 'src/shared/actions/notifications'
import {presentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'

import {TimeZone, Theme, CurrentPage} from 'src/types'

export enum ActionTypes {
  EnablePresentationMode = 'ENABLE_PRESENTATION_MODE',
  DisablePresentationMode = 'DISABLE_PRESENTATION_MODE',
  SetAutoRefresh = 'SET_AUTOREFRESH',
  SetTimeZone = 'SET_APP_TIME_ZONE',
  TemplateControlBarVisibilityToggled = 'TemplateControlBarVisibilityToggledAction',
  Noop = 'NOOP',
}

export type Action =
  | EnablePresentationModeAction
  | DisablePresentationModeAction
  | SetAutoRefreshAction
  | SetTimeZoneAction
  | TemplateControlBarVisibilityToggledAction
  | ReturnType<typeof setTheme>
  | ReturnType<typeof setCurrentPage>

export const setCurrentPage = (currentPage: CurrentPage) =>
  ({type: 'SET_CURRENT_PAGE', currentPage} as const)

export const setTheme = (theme: Theme) => ({type: 'SET_THEME', theme} as const)

export type EnablePresentationModeActionCreator = () => EnablePresentationModeAction

export interface SetTimeZoneAction {
  type: ActionTypes.SetTimeZone
  payload: {timeZone: TimeZone}
}

export interface EnablePresentationModeAction {
  type: ActionTypes.EnablePresentationMode
}

export interface DisablePresentationModeAction {
  type: ActionTypes.DisablePresentationMode
}

export type DelayEnablePresentationModeDispatcher = () => DelayEnablePresentationModeThunk

export type DelayEnablePresentationModeThunk = (
  dispatch: Dispatch<EnablePresentationModeAction>
) => NodeJS.Timer

export type SetAutoRefreshActionCreator = (
  milliseconds: number
) => SetAutoRefreshAction

export interface SetAutoRefreshAction {
  type: ActionTypes.SetAutoRefresh
  payload: {
    milliseconds: number
  }
}

export type TemplateControlBarVisibilityToggledActionCreator = () => TemplateControlBarVisibilityToggledAction

export interface TemplateControlBarVisibilityToggledAction {
  type: ActionTypes.TemplateControlBarVisibilityToggled
}

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

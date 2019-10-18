import {Dispatch} from 'redux'

import {TimeZone} from 'src/types'

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

import {TimeMachineState} from 'src/shared/reducers/v2/timeMachines'
import {TimeRange, ViewType} from 'src/types/v2'

export type Action =
  | SetActiveTimeMachineAction
  | SetNameAction
  | SetTimeRangeAction
  | SetTypeAction
  | SetDraftScriptAction
  | SubmitScriptAction

interface SetActiveTimeMachineAction {
  type: 'SET_ACTIVE_TIME_MACHINE'
  payload: {
    activeTimeMachineID: string
    initialState: Partial<TimeMachineState>
  }
}

export const setActiveTimeMachine = (
  activeTimeMachineID: string,
  initialState: Partial<TimeMachineState> = {}
): SetActiveTimeMachineAction => ({
  type: 'SET_ACTIVE_TIME_MACHINE',
  payload: {activeTimeMachineID, initialState},
})

interface SetNameAction {
  type: 'SET_VIEW_NAME'
  payload: {name: string}
}

export const setName = (name: string): SetNameAction => ({
  type: 'SET_VIEW_NAME',
  payload: {name},
})

interface SetTimeRangeAction {
  type: 'SET_TIME_RANGE'
  payload: {timeRange: TimeRange}
}

export const setTimeRange = (timeRange: TimeRange): SetTimeRangeAction => ({
  type: 'SET_TIME_RANGE',
  payload: {timeRange},
})

interface SetTypeAction {
  type: 'SET_VIEW_TYPE'
  payload: {type: ViewType}
}

export const setType = (type: ViewType): SetTypeAction => ({
  type: 'SET_VIEW_TYPE',
  payload: {type},
})

interface SetDraftScriptAction {
  type: 'SET_DRAFT_SCRIPT'
  payload: {draftScript: string}
}

export const setDraftScript = (draftScript: string): SetDraftScriptAction => ({
  type: 'SET_DRAFT_SCRIPT',
  payload: {draftScript},
})

interface SubmitScriptAction {
  type: 'SUBMIT_SCRIPT'
}

export const submitScript = (): SubmitScriptAction => ({
  type: 'SUBMIT_SCRIPT',
})

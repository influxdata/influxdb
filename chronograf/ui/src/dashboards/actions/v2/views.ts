export type Action = SetActiveViewAction

export enum ActionTypes {
  SetActiveView = 'SET_ACTIVE_VIEW',
}

interface SetActiveViewAction {
  type: ActionTypes.SetActiveView
  payload: {
    activeViewID: string
  }
}

export const setActiveCell = (activeViewID: string): SetActiveViewAction => ({
  type: ActionTypes.SetActiveView,
  payload: {activeViewID},
})

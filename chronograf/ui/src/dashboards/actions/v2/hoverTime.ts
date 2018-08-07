export enum ActionTypes {
  SetHoverTime = 'SET_HOVER_TIME',
}

export type Action = SetHoverTimeAction

interface SetHoverTimeAction {
  type: ActionTypes.SetHoverTime
  payload: {
    hoverTime: string
  }
}

export const setHoverTime = (hoverTime: string): SetHoverTimeAction => ({
  type: ActionTypes.SetHoverTime,
  payload: {hoverTime},
})

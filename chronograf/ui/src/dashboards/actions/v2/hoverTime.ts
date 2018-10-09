export type Action = SetHoverTimeAction

interface SetHoverTimeAction {
  type: 'SET_HOVER_TIME'
  payload: {
    hoverTime: string
  }
}

export const setHoverTime = (hoverTime: string): SetHoverTimeAction => ({
  type: 'SET_HOVER_TIME',
  payload: {hoverTime},
})

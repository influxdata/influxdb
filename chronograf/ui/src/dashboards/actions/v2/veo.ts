export type Action = SetViewName

interface SetViewName {
  type: 'SET_VIEW_NAME'
  payload: {
    viewName: string
  }
}

export const setViewName = (viewName: string): SetViewName => {
  return {
    type: 'SET_VIEW_NAME',
    payload: {viewName},
  }
}

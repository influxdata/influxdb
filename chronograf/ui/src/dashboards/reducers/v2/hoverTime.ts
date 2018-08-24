import {Action, ActionTypes} from 'src/dashboards/actions/v2/hoverTime'

const initialState = '0'

export default (state: string = initialState, action: Action): string => {
  switch (action.type) {
    case ActionTypes.SetHoverTime: {
      const {hoverTime} = action.payload

      return hoverTime
    }
  }

  return state
}

import {Action} from 'src/dashboards/actions/v2/hoverTime'

export type HoverTimeState = string

const INITIAL_STATE = '0'

export default (
  state: HoverTimeState = INITIAL_STATE,
  action: Action
): string => {
  switch (action.type) {
    case 'SET_HOVER_TIME': {
      const {hoverTime} = action.payload

      return hoverTime
    }
  }

  return state
}

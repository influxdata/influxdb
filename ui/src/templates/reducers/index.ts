import {Actions, ActionTypes} from 'src/templates/actions/'
import {TemplateSummary} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

export interface TemplatesState {
  status: RemoteDataState
  items: TemplateSummary[]
}

const defaultState: TemplatesState = {
  status: RemoteDataState.NotStarted,
  items: [],
}

const templatesReducer = (
  state = defaultState,
  action: Actions
): TemplatesState => {
  switch (action.type) {
    case ActionTypes.PopulateTemplateSummaries:
      return {
        ...state,
        items: action.payload.items,
        status: action.payload.status,
      }
    case ActionTypes.SetTemplatesStatus:
      return {
        ...state,
        status: action.payload.status,
      }
    default:
      return state
  }
}

export default templatesReducer

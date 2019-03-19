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
        status: RemoteDataState.Done,
      }
    case ActionTypes.SetStatusToLoading:
      return {
        ...state,
        status: RemoteDataState.Loading,
      }
    default:
      return state
  }
}

export default templatesReducer

import {ITask as Task} from '@influxdata/influx'
import {Dashboard} from 'src/types/v2'
import {Actions, ActionTypes} from 'src/organizations/actions/orgView'

export interface OrgViewState {
  tasks: Task[]
  dashboards: Dashboard[]
}

const defaultState: OrgViewState = {
  tasks: [],
  dashboards: [],
}

export default (state = defaultState, action: Actions): OrgViewState => {
  switch (action.type) {
    case ActionTypes.PopulateTasks:
      return {...state, tasks: action.payload.tasks}
    case ActionTypes.PopulateDashboards:
      return {...state, dashboards: action.payload.dashboards}
    default:
      return state
  }
}

import {ITask as Task, Telegraf} from '@influxdata/influx'
import {Dashboard} from 'src/types'
import {Actions, ActionTypes} from 'src/organizations/actions/orgView'

export interface OrgViewState {
  tasks: Task[]
  dashboards: Dashboard[]
  telegrafs: Telegraf[]
}

const defaultState: OrgViewState = {
  tasks: [],
  dashboards: [],
  telegrafs: [],
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

import {ITask as Task, Telegraf} from '@influxdata/influx'
import {Dashboard} from 'src/types/v2'
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
    case 'ADD_DASHBOARD_LABELS': {
      const {dashboardID, labels} = action.payload

      const dashboards = state.dashboards.map(d => {
        if (d.id === dashboardID) {
          return {...d, labels: [...d.labels, ...labels]}
        }
        return d
      })

      return {...state, dashboards}
    }
    case 'REMOVE_DASHBOARD_LABELS': {
      const {dashboardID, labels} = action.payload

      const dashboards = state.dashboards.map(d => {
        if (d.id === dashboardID) {
          const updatedLabels = d.labels.filter(l => {
            return !labels.includes(l)
          })
          return {...d, labels: updatedLabels}
        }
        return d
      })

      return {...state, dashboards}
    }
    default:
      return state
  }
}

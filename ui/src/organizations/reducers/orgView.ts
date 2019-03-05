import {
  Variable,
  Telegraf,
  ScraperTargetResponse,
  Bucket,
  Task,
} from '@influxdata/influx'
import {Dashboard} from 'src/types'
import {Actions, ActionTypes} from 'src/organizations/actions/orgView'
import {addTaskLabels, removeTaskLabels} from 'src/tasks/reducers/v2/utils'

export interface OrgViewState {
  tasks: Task[]
  telegrafs: Telegraf[]
  scrapers: ScraperTargetResponse[]
  variables: Variable[]
  dashboards: Dashboard[]
  buckets: Bucket[]
}

const defaultState: OrgViewState = {
  tasks: [],
  telegrafs: [],
  scrapers: [],
  variables: [],
  dashboards: [],
  buckets: [],
}

export default (state = defaultState, action: Actions): OrgViewState => {
  switch (action.type) {
    case ActionTypes.PopulateTasks:
      return {...state, tasks: action.payload.tasks}
    case ActionTypes.AddTaskLabels: {
      const {taskID, labels} = action.payload

      return {...state, tasks: addTaskLabels(state.tasks, taskID, labels)}
    }
    case ActionTypes.RemoveTaskLabels: {
      const {taskID, labels} = action.payload

      return {...state, tasks: removeTaskLabels(state.tasks, taskID, labels)}
    }
    default:
      return state
  }
}

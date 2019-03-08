import {
  Variable,
  Telegraf,
  ScraperTargetResponse,
  Bucket,
  ITask as Task,
} from '@influxdata/influx'
import {Dashboard} from 'src/types'
import {Actions, ActionTypes} from 'src/organizations/actions/orgView'

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
    case ActionTypes.UpdateTask: {
      const {task} = action.payload
      const tasks = state.tasks.map(t => (t.id === task.id ? task : t))

      return {...state, tasks}
    }
    default:
      return state
  }
}

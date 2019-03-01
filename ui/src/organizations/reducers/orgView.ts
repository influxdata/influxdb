import {
  Variable,
  Telegraf,
  ScraperTargetResponse,
  Bucket,
} from '@influxdata/influx'
import {Task} from 'src/types/v2'
import {Dashboard} from 'src/types'
import {ActionTypes, Actions} from 'src/organizations/actions/orgView'

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
    default:
      return state
  }
}

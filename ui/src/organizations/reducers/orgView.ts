import {ITask as Task, Telegraf} from '@influxdata/influx'
import {Actions, ActionTypes} from 'src/organizations/actions/orgView'

export interface OrgViewState {
  tasks: Task[]
  telegrafs: Telegraf[]
}

const defaultState: OrgViewState = {
  tasks: [],
  telegrafs: [],
}

export default (state = defaultState, action: Actions): OrgViewState => {
  switch (action.type) {
    case ActionTypes.PopulateTasks:
      return {...state, tasks: action.payload.tasks}
    default:
      return state
  }
}

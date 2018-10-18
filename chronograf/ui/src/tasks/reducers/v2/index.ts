import {Action, ActionTypes} from 'src/tasks/actions/v2'
import {Task} from 'src/types/v2/tasks'

export interface State {
  newScript: string
  tasks: Task[]
  searchTerm: string
}

const defaultState: State = {
  newScript: '',
  tasks: [],
  searchTerm: '',
}

export default (state: State = defaultState, action: Action): State => {
  switch (action.type) {
    case ActionTypes.SetNewScript:
      return {...state, newScript: action.payload.script}
    case ActionTypes.SetTasks:
      return {...state, tasks: action.payload.tasks}
    case ActionTypes.SetSearchTerm:
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    default:
      return state
  }
}

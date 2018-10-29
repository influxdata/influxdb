import {Action, ActionTypes} from 'src/tasks/actions/v2'
import {Task} from 'src/types/v2/tasks'

export interface State {
  newScript: string
  currentScript: string
  currentTask?: Task
  tasks: Task[]
  searchTerm: string
}

const defaultState: State = {
  newScript: '',
  currentScript: '',
  tasks: [],
  searchTerm: '',
}

export default (state: State = defaultState, action: Action): State => {
  switch (action.type) {
    case ActionTypes.SetNewScript:
      return {...state, newScript: action.payload.script}
    case ActionTypes.SetCurrentScript:
      return {...state, currentScript: action.payload.script}
    case ActionTypes.SetCurrentTask:
      const {task} = action.payload
      let currentScript = ''
      if (task) {
        currentScript = task.flux
      }
      return {...state, currentScript, currentTask: task}
    case ActionTypes.SetTasks:
      return {...state, tasks: action.payload.tasks}
    case ActionTypes.SetSearchTerm:
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    default:
      return state
  }
}

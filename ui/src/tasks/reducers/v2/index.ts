import {Action} from 'src/tasks/actions/v2'
import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {Task as TaskAPI, User, Organization} from 'src/api'

interface Task extends TaskAPI {
  organization: Organization
  owner?: User
  delay?: string
}
export interface State {
  newScript: string
  currentScript: string
  currentTask?: Task
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  dropdownOrgID: string
  interval: string
  taskOptions: TaskOptions
}

const defaultTaskOptions: TaskOptions = {
  name: '',
  intervalTime: '1',
  intervalUnit: 'd',
  delayTime: '20',
  delayUnit: 'm',
  cron: '',
  taskScheduleType: TaskSchedule.interval,
  orgID: null,
}

const defaultState: State = {
  newScript: '',
  currentScript: '',
  tasks: [],
  searchTerm: '',
  showInactive: true,
  dropdownOrgID: null,
  interval: '1d',
  taskOptions: defaultTaskOptions,
}

export default (state: State = defaultState, action: Action): State => {
  switch (action.type) {
    case 'CLEAR_TASK_OPTIONS':
      return {
        ...state,
        taskOptions: defaultTaskOptions,
      }
    case 'SET_TASK_OPTION':
      const {key, value} = action.payload

      return {
        ...state,
        taskOptions: {...state.taskOptions, [key]: value},
      }
    case 'SET_SCHEDULE_UNIT':
      const {unit, schedule} = action.payload

      return {
        ...state,
        taskOptions: {...state.taskOptions, [schedule]: unit},
      }
    case 'SET_NEW_SCRIPT':
      return {...state, newScript: action.payload.script}
    case 'SET_CURRENT_SCRIPT':
      return {...state, currentScript: action.payload.script}
    case 'SET_CURRENT_TASK':
      const {task} = action.payload
      let currentScript = ''
      if (task) {
        currentScript = task.flux
      }
      return {...state, currentScript, currentTask: task}
    case 'SET_TASKS':
      return {...state, tasks: action.payload.tasks}
    case 'SET_SEARCH_TERM':
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    case 'SET_SHOW_INACTIVE':
      return {...state, showInactive: !state.showInactive}
    case 'SET_DROPDOWN_ORG_ID':
      const {dropdownOrgID} = action.payload
      return {...state, dropdownOrgID}
    default:
      return state
  }
}

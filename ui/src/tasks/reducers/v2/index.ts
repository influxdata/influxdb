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
  taskOptions: TaskOptions
}

const defaultTaskOptions: TaskOptions = {
  name: '',
  interval: '',
  delay: '',
  cron: '',
  taskScheduleType: TaskSchedule.unselected,
  orgID: null,
}

const defaultState: State = {
  newScript: '',
  currentScript: '',
  tasks: [],
  searchTerm: '',
  showInactive: true,
  dropdownOrgID: null,
  taskOptions: defaultTaskOptions,
}

export default (state: State = defaultState, action: Action): State => {
  switch (action.type) {
    case 'CLEAR_TASK':
      return {
        ...state,
        taskOptions: defaultTaskOptions,
        currentScript: '',
        newScript: '',
      }
    case 'SET_ALL_TASK_OPTIONS':
      const {name, every, cron, organizationId, delay} = action.payload
      let taskScheduleType = TaskSchedule.interval
      if (cron) {
        taskScheduleType = TaskSchedule.cron
      }

      return {
        ...state,
        taskOptions: {
          ...state.taskOptions,
          name,
          cron,
          interval: every,
          orgID: organizationId,
          taskScheduleType,
          delay,
        },
      }
    case 'SET_TASK_OPTION':
      const {key, value} = action.payload

      return {
        ...state,
        taskOptions: {...state.taskOptions, [key]: value},
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

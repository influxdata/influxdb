import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'

// Types
import {Action} from 'src/tasks/actions'
import {Task, LogEvent, Run} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

export interface TasksState {
  status: RemoteDataState
  list: Task[]
  newScript: string
  currentScript: string
  currentTask: Task
  searchTerm: string
  showInactive: boolean
  taskOptions: TaskOptions
  runs: Run[]
  runStatus: RemoteDataState
  logs: LogEvent[]
}

export const defaultTaskOptions: TaskOptions = {
  name: '',
  interval: '',
  offset: '',
  cron: '',
  taskScheduleType: TaskSchedule.unselected,
  orgID: '',
  toBucketName: '',
  toOrgName: '',
}

export const defaultState: TasksState = {
  status: RemoteDataState.NotStarted,
  list: [],
  newScript: '',
  currentTask: null,
  currentScript: '',
  searchTerm: '',
  showInactive: true,
  taskOptions: defaultTaskOptions,
  runs: [],
  runStatus: RemoteDataState.NotStarted,
  logs: [],
}

export default (
  state: TasksState = defaultState,
  action: Action
): TasksState => {
  switch (action.type) {
    case 'SET_TASKS':
      return {
        ...state,
        list: action.payload.tasks,
        status: RemoteDataState.Done,
      }
    case 'SET_TASKS_STATUS':
      return {
        ...state,
        status: action.payload.status,
      }
    case 'CLEAR_TASK':
      return {
        ...state,
        taskOptions: defaultTaskOptions,
        currentScript: '',
        newScript: '',
      }
    case 'SET_ALL_TASK_OPTIONS':
      const {name, every, cron, orgID, offset} = action.payload
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
          orgID,
          taskScheduleType,
          offset,
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
    case 'SET_SEARCH_TERM':
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    case 'SET_SHOW_INACTIVE':
      return {...state, showInactive: !state.showInactive}
    case 'UPDATE_TASK': {
      const {task} = action.payload
      const tasks = state.list.map(t => (t.id === task.id ? task : t))

      return {...state, list: tasks}
    }
    case 'SET_RUNS':
      const {runs, runStatus} = action.payload
      return {...state, runs, runStatus}
    case 'SET_LOGS':
      const {logs} = action.payload
      return {...state, logs}
    default:
      return state
  }
}

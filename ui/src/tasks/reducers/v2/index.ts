import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {Run, LogEvent} from '@influxdata/influx'

// Types
import {Action} from 'src/tasks/actions/v2'
import {Task} from '@influxdata/influx'
import {RemoteDataState} from '@influxdata/clockface'

export interface State {
  newScript: string
  currentScript: string
  currentTask?: Task
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  dropdownOrgID: string
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

const defaultState: State = {
  newScript: '',
  currentScript: '',
  tasks: [],
  searchTerm: '',
  showInactive: true,
  dropdownOrgID: null,
  taskOptions: defaultTaskOptions,
  runs: [],
  runStatus: RemoteDataState.NotStarted,
  logs: [],
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
    case 'ADD_TASK_LABELS':
      const {taskID, labels} = action.payload

      const updatedTasks = state.tasks.map(t => {
        if (t.id === taskID) {
          return {...t, labels: [...labels]}
        }
        return t
      })

      return {...state, tasks: [...updatedTasks]}
    case 'REMOVE_TASK_LABELS': {
      const {taskID, labels} = action.payload

      const updatedTasks = state.tasks.map(t => {
        if (t.id === taskID) {
          const updatedLabels = t.labels.filter(l => {
            if (!labels.find(label => label.name === l.name)) {
              return l
            }
          })

          return {...t, labels: updatedLabels}
        }
        return t
      })

      return {...state, tasks: [...updatedTasks]}
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

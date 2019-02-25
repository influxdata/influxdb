// Libraries
import {push, goBack} from 'react-router-redux'
import _ from 'lodash'

// APIs
import {Task as TaskAPI, Organization, Run} from '@influxdata/influx'
import {client} from 'src/utils/api'
import {notify} from 'src/shared/actions/notifications'
import {
  taskNotCreated,
  tasksFetchFailed,
  taskDeleteFailed,
  taskNotFound,
  taskUpdateFailed,
  taskImportFailed,
  taskImportSuccess,
  taskUpdateSuccess,
  taskCreatedSuccess,
  taskDeleteSuccess,
  taskCloneSuccess,
  taskCloneFailed,
  taskRunSuccess,
} from 'src/shared/copy/v2/notifications'

// Types
import {AppState, Label} from 'src/types/v2'

// Utils
import {getDeep} from 'src/utils/wrappers'
import {
  taskOptionsToFluxScript,
  addDestinationToFluxScript,
  TaskOptionKeys,
  TaskOptions,
  TaskSchedule,
} from 'src/utils/taskOptionsToFluxScript'

export type Action =
  | SetNewScript
  | SetTasks
  | SetSearchTerm
  | SetCurrentScript
  | SetCurrentTask
  | SetShowInactive
  | SetDropdownOrgID
  | SetTaskInterval
  | SetTaskCron
  | ClearTask
  | SetTaskOption
  | SetAllTaskOptions
  | AddTaskLabels
  | RemoveTaskLabels
  | SetRuns

type GetStateFunc = () => AppState
interface Task extends TaskAPI {
  organization: Organization
}

export interface SetAllTaskOptions {
  type: 'SET_ALL_TASK_OPTIONS'
  payload: Task
}

export interface ClearTask {
  type: 'CLEAR_TASK'
}

export interface SetTaskInterval {
  type: 'SET_TASK_INTERVAL'
  payload: {
    interval: string
  }
}

export interface SetTaskCron {
  type: 'SET_TASK_CRON'
  payload: {
    cron: string
  }
}

export interface SetNewScript {
  type: 'SET_NEW_SCRIPT'
  payload: {
    script: string
  }
}
export interface SetCurrentScript {
  type: 'SET_CURRENT_SCRIPT'
  payload: {
    script: string
  }
}
export interface SetCurrentTask {
  type: 'SET_CURRENT_TASK'
  payload: {
    task: Task
  }
}

export interface SetTasks {
  type: 'SET_TASKS'
  payload: {
    tasks: Task[]
  }
}

export interface SetSearchTerm {
  type: 'SET_SEARCH_TERM'
  payload: {
    searchTerm: string
  }
}

export interface SetShowInactive {
  type: 'SET_SHOW_INACTIVE'
  payload: {}
}

export interface SetDropdownOrgID {
  type: 'SET_DROPDOWN_ORG_ID'
  payload: {
    dropdownOrgID: string
  }
}

export interface SetTaskOption {
  type: 'SET_TASK_OPTION'
  payload: {
    key: TaskOptionKeys
    value: string
  }
}

export interface AddTaskLabels {
  type: 'ADD_TASK_LABELS'
  payload: {
    taskID: string
    labels: Label[]
  }
}

export interface RemoveTaskLabels {
  type: 'REMOVE_TASK_LABELS'
  payload: {
    taskID: string
    labels: Label[]
  }
}

export interface SetRuns {
  type: 'SET_RUNS'
  payload: {
    runs: Run[]
  }
}

export const setTaskOption = (taskOption: {
  key: TaskOptionKeys
  value: string
}): SetTaskOption => ({
  type: 'SET_TASK_OPTION',
  payload: {...taskOption},
})

export const setAllTaskOptions = (task: Task): SetAllTaskOptions => ({
  type: 'SET_ALL_TASK_OPTIONS',
  payload: {...task},
})

export const clearTask = (): ClearTask => ({
  type: 'CLEAR_TASK',
})

export const setNewScript = (script: string): SetNewScript => ({
  type: 'SET_NEW_SCRIPT',
  payload: {script},
})

export const setCurrentScript = (script: string): SetCurrentScript => ({
  type: 'SET_CURRENT_SCRIPT',
  payload: {script},
})

export const setCurrentTask = (task: Task): SetCurrentTask => ({
  type: 'SET_CURRENT_TASK',
  payload: {task},
})

export const setTasks = (tasks: Task[]): SetTasks => ({
  type: 'SET_TASKS',
  payload: {tasks},
})

export const setSearchTerm = (searchTerm: string): SetSearchTerm => ({
  type: 'SET_SEARCH_TERM',
  payload: {searchTerm},
})

export const setShowInactive = (): SetShowInactive => ({
  type: 'SET_SHOW_INACTIVE',
  payload: {},
})

export const setDropdownOrgID = (dropdownOrgID: string): SetDropdownOrgID => ({
  type: 'SET_DROPDOWN_ORG_ID',
  payload: {dropdownOrgID},
})

export const setRuns = (runs: Run[]): SetRuns => ({
  type: 'SET_RUNS',
  payload: {runs},
})

const addTaskLabels = (taskID: string, labels: Label[]): AddTaskLabels => ({
  type: 'ADD_TASK_LABELS',
  payload: {
    taskID,
    labels,
  },
})

const removeTaskLabels = (
  taskID: string,
  labels: Label[]
): RemoveTaskLabels => ({
  type: 'REMOVE_TASK_LABELS',
  payload: {
    taskID,
    labels,
  },
})

// Thunks

export const updateTaskStatus = (task: Task) => async dispatch => {
  try {
    await client.tasks.updateStatus(task.id, task.status)

    dispatch(populateTasks())
    dispatch(notify(taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskUpdateFailed(message)))
  }
}

export const deleteTask = (task: Task) => async dispatch => {
  try {
    await client.tasks.delete(task.id)

    dispatch(populateTasks())
    dispatch(notify(taskDeleteSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskDeleteFailed(message)))
  }
}

export const cloneTask = (task: Task, _) => async dispatch => {
  try {
    await client.tasks.clone(task.id)

    dispatch(notify(taskCloneSuccess(task.name)))
    dispatch(populateTasks())
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskCloneFailed(task.name, message)))
  }
}

export const populateTasks = () => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {orgs} = getState()

    const user = await client.users.me()
    const tasks = await client.tasks.getAllByUser(user)

    const mappedTasks = tasks.map(task => {
      const org = orgs.find(org => org.id === task.orgID)

      return {
        ...task,
        organization: org,
      }
    })

    dispatch(setTasks(mappedTasks))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(tasksFetchFailed(message)))
  }
}

export const selectTaskByID = (id: string, route?: string) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {orgs} = getState()

    const task = await client.tasks.get(id)
    const org = orgs.find(org => org.id === task.orgID)

    return dispatch(setCurrentTask({...task, organization: org}))
  } catch (e) {
    console.error(e)
    dispatch(goToTasks(route))
    const message = getErrorMessage(e)
    dispatch(notify(taskNotFound(message)))
  }
}

export const selectTask = (task: Task, route?: string) => async dispatch => {
  if (route) {
    dispatch(push(route))
    return
  }
  dispatch(push(`/tasks/${task.id}`))
}

export const goToTasks = (route?: string) => async dispatch => {
  if (route) {
    dispatch(push(route))
    return
  }
  dispatch(push('/tasks'))
}

export const cancel = () => async dispatch => {
  dispatch(setCurrentTask(null))
  dispatch(goBack())
}

export const updateScript = (route?: string) => async (
  dispatch,
  getState: GetStateFunc
) => {
  try {
    const {
      tasks: {currentScript: script, currentTask: task, taskOptions},
    } = getState()

    const updatedTask: Partial<TaskAPI> & {name: string; flux: string} = {
      flux: script,
      name: taskOptions.name,
      offset: taskOptions.offset,
    }

    if (taskOptions.taskScheduleType === TaskSchedule.interval) {
      updatedTask.every = taskOptions.interval
    } else {
      updatedTask.cron = taskOptions.cron
    }

    await client.tasks.update(task.id, updatedTask)

    dispatch(setCurrentTask(null))
    dispatch(goToTasks(route))
    dispatch(notify(taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskUpdateFailed(message)))
  }
}

export const saveNewScript = (
  script: string,
  taskOptions: TaskOptions,
  route?: string
) => async (dispatch, getState: GetStateFunc): Promise<void> => {
  try {
    const {orgs} = await getState()

    const fluxTaskOptions = taskOptionsToFluxScript(taskOptions)
    const scriptWithOptions = addDestinationToFluxScript(
      `${fluxTaskOptions}\n\n${script}`,
      taskOptions
    )

    let org = orgs.find(org => {
      return org.id === taskOptions.orgID
    })

    if (!org) {
      org = orgs[0]
    }

    await client.tasks.create(org.name, scriptWithOptions)

    dispatch(setNewScript(''))
    dispatch(clearTask())
    dispatch(populateTasks())
    dispatch(goToTasks(route))
    dispatch(notify(taskCreatedSuccess()))
  } catch (e) {
    console.error(e)
    const message = _.get(e, 'response.data.error.message', '')
    dispatch(notify(taskNotCreated(message)))
  }
}

export const importTask = (script: string) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    if (_.isEmpty(script)) {
      dispatch(notify(taskImportFailed('File is empty')))
      return
    }

    const {orgs} = await getState()
    const orgID = getDeep<string>(orgs, '0.id', '') // TODO org selection by user.

    await client.tasks.create(orgID, script)

    dispatch(populateTasks())

    dispatch(notify(taskImportSuccess()))
  } catch (error) {
    console.error(error)
    const message = _.get(error, 'response.data.error.message', '')
    dispatch(notify(taskImportFailed(message)))
  }
}

export const addTaskLabelsAsync = (taskID: string, labels: Label[]) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {
      tasks: {tasks},
    } = await getState()

    const task = tasks.find(t => {
      return t.id === taskID
    })

    const labelsToAdd = labels.filter(label => {
      if (!task.labels.find(l => l.name === label.name)) {
        return label
      }
    })

    const newLabels = await client.tasks.addLabels(taskID, labelsToAdd)

    dispatch(addTaskLabels(taskID, newLabels))
  } catch (error) {
    console.error(error)
  }
}

export const removeTaskLabelsAsync = (
  taskID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.tasks.removeLabels(taskID, labels)
    dispatch(removeTaskLabels(taskID, labels))
  } catch (error) {
    console.error(error)
  }
}

export const getErrorMessage = (e: any) => {
  let message = _.get(e, 'response.data.error.message', '')
  if (message === '') {
    message = _.get(e, 'response.headers.x-influx-error', '')
  }
  return message
}

export const getRuns = (taskID: string) => async (dispatch): Promise<void> => {
  try {
    const runs = await client.tasks.getRunsByTaskID(taskID)

    dispatch(setRuns(runs))
  } catch (error) {
    console.error(error)
  }
}

export const runTask = (taskID: string) => async dispatch => {
  try {
    await client.tasks.startRunByTaskID(taskID)
    dispatch(notify(taskRunSuccess()))
  } catch (e) {
    console.error(e)
  }
}

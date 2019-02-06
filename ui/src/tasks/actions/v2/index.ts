// Libraries
import {push} from 'react-router-redux'
import _ from 'lodash'

// APIs
import {Task as TaskAPI, Organization} from '@influxdata/influx'
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
    // const allTaskNames = tasks.map(t => t.name)
    // const clonedName = incrementCloneName(allTaskNames, task.name)
    await client.tasks.create(task.orgID, task.flux)

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

export const selectTaskByID = (id: string) => async (
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
    dispatch(goToTasks())
    const message = getErrorMessage(e)
    dispatch(notify(taskNotFound(message)))
  }
}

export const selectTask = (task: Task) => async dispatch => {
  dispatch(push(`/tasks/${task.id}`))
}

export const goToTasks = () => async dispatch => {
  dispatch(push('/tasks'))
}

export const cancelUpdateTask = () => async dispatch => {
  dispatch(setCurrentTask(null))
  dispatch(goToTasks())
}

export const updateScript = () => async (dispatch, getState: GetStateFunc) => {
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
    dispatch(goToTasks())
    dispatch(notify(taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskUpdateFailed(message)))
  }
}

export const saveNewScript = (
  script: string,
  taskOptions: TaskOptions
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

    await client.tasks.create(getDeep<string>(org, 'id', ''), scriptWithOptions)

    dispatch(setNewScript(''))
    dispatch(clearTask())
    dispatch(goToTasks())
    dispatch(populateTasks())
    dispatch(notify(taskCreatedSuccess()))
  } catch (e) {
    console.error(e)
    const message = _.get(e, 'response.data.error.message', '')
    dispatch(notify(taskNotCreated(message)))
  }
}

export const importScript = (script: string, fileName: string) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const validFileExtension = '.flux'

    const fileExtensionRegex = new RegExp(`${validFileExtension}$`)

    if (!fileName.match(fileExtensionRegex)) {
      dispatch(notify(taskImportFailed(fileName, 'Please import a .flux file')))
      return
    }

    if (_.isEmpty(script)) {
      dispatch(notify(taskImportFailed(fileName, 'No Flux found in file')))
      return
    }

    const {orgs} = await getState()
    const orgID = getDeep<string>(orgs, '0.id', '') // TODO org selection by user.

    await client.tasks.create(orgID, script)

    dispatch(populateTasks())

    dispatch(notify(taskImportSuccess(fileName)))
  } catch (error) {
    console.error(error)
    const message = _.get(error, 'response.data.error.message', '')
    dispatch(notify(taskImportFailed(fileName, message)))
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

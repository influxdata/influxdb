import {AppState} from 'src/types/v2'
import {push} from 'react-router-redux'
import _ from 'lodash'

import {Task as TaskAPI, Organization} from 'src/api'
import {
  submitNewTask,
  updateTaskFlux,
  getUserTasks,
  getTask,
  updateTaskStatus as updateTaskStatusAPI,
  deleteTask as deleteTaskAPI,
} from 'src/tasks/api/v2'
import {getMe} from 'src/shared/apis/v2/user'
import {notify} from 'src/shared/actions/notifications'
import {
  taskNotCreated,
  tasksFetchFailed,
  taskDeleteFailed,
  taskNotFound,
  taskUpdateFailed,
  taskImportFailed,
  taskImportSuccess,
} from 'src/shared/copy/v2/notifications'
import {getDeep} from 'src/utils/wrappers'

import {
  taskOptionsToFluxScript,
  TaskOptionKeys,
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

export const updateTaskStatus = (task: Task) => async dispatch => {
  try {
    await updateTaskStatusAPI(task.id, task.status)

    dispatch(populateTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskDeleteFailed()))
  }
}

export const deleteTask = (task: Task) => async dispatch => {
  try {
    await deleteTaskAPI(task.id)

    dispatch(populateTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskDeleteFailed()))
  }
}

export const populateTasks = () => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {
      orgs,
      links: {me: meUrl},
    } = getState()

    const user = await getMe(meUrl)
    const tasks = await getUserTasks(user)

    const mappedTasks = tasks.map(task => {
      const org = orgs.find(org => org.id === task.organizationId)

      return {
        ...task,
        organization: org,
      }
    })

    dispatch(setTasks(mappedTasks))
  } catch (e) {
    console.error(e)
    dispatch(notify(tasksFetchFailed()))
  }
}

export const selectTaskByID = (id: string) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {orgs} = getState()

    const task = await getTask(id)
    const org = orgs.find(org => org.id === task.organizationId)

    return dispatch(setCurrentTask({...task, organization: org}))
  } catch (e) {
    console.error(e)
    dispatch(goToTasks())
    dispatch(notify(taskNotFound()))
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
      tasks: {currentScript: script, currentTask: task},
    } = getState()

    await updateTaskFlux(task.id, script)

    dispatch(setCurrentTask(null))
    dispatch(goToTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskUpdateFailed()))
  }
}

export const saveNewScript = () => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {
      orgs,
      tasks: {newScript: script, taskOptions},
    } = await getState()

    const fluxTaskOptions = taskOptionsToFluxScript(taskOptions)
    const scriptWithOptions = `${fluxTaskOptions}\n${script}`

    let org = orgs.find(org => {
      return org.id === taskOptions.orgID
    })

    if (!org) {
      org = orgs[0]
    }

    await submitNewTask(getDeep<string>(org, 'id', ''), scriptWithOptions)

    dispatch(setNewScript(''))
    dispatch(clearTask())
    dispatch(goToTasks())
    dispatch(populateTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskNotCreated(e.headers['x-influx-error'])))
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

    await submitNewTask(orgID, script)

    dispatch(populateTasks())

    dispatch(notify(taskImportSuccess(fileName)))
  } catch (error) {
    console.error(error)
    const explanation = getDeep<string>(
      error,
      'response.headers.x-influx-error',
      ''
    )
    dispatch(notify(taskImportFailed(fileName, explanation)))
  }
}

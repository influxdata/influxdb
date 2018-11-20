import {AppState} from 'src/types/v2'
import {push} from 'react-router-redux'

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
} from 'src/shared/copy/v2/notifications'
import {getDeep} from 'src/utils/wrappers'

import {taskOptionsToFluxScript} from 'src/utils/taskOptionsToFluxScript'

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
  | ClearTaskOptions
  | SetTaskOption
  | SetScheduleUnit

type GetStateFunc = () => AppState
interface Task extends TaskAPI {
  organization: Organization
}

export enum ActionTypes {
  SetNewScript = 'SET_NEW_SCRIPT',
  SetTasks = 'SET_TASKS',
  SetSearchTerm = 'SET_TASKS_SEARCH_TERM',
  SetCurrentScript = 'SET_CURRENT_SCRIPT',
  SetCurrentTask = 'SET_CURRENT_TASK',
  SetShowInactive = 'SET_TASKS_SHOW_INACTIVE',
  SetDropdownOrgID = 'SET_DROPDOWN_ORG_ID',
}
export interface ClearTaskOptions {
  type: 'CLEAR_TASK_OPTIONS'
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
    key: string
    value: string
  }
}

export interface SetScheduleUnit {
  type: 'SET_SCHEDULE_UNIT'
  payload: {
    schedule: string
    unit: string
  }
}

export const setTaskOption = (taskOption: {
  key: string
  value: string
}): SetTaskOption => ({
  type: 'SET_TASK_OPTION',
  payload: {...taskOption},
})

export const clearTaskOptions = (): ClearTaskOptions => ({
  type: 'CLEAR_TASK_OPTIONS',
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

export const setScheduleUnit = (
  unit: string,
  schedule: string
): SetScheduleUnit => ({
  type: 'SET_SCHEDULE_UNIT',
  payload: {unit, schedule},
})

export const updateTaskStatus = (task: Task) => async (
  dispatch,
  getState: GetStateFunc
) => {
  try {
    const {
      links: {tasks: url},
    } = getState()
    await updateTaskStatusAPI(url, task.id, task.status)

    dispatch(populateTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskDeleteFailed()))
  }
}

export const deleteTask = (task: Task) => async (
  dispatch,
  getState: GetStateFunc
) => {
  try {
    const {
      links: {tasks: url},
    } = getState()

    await deleteTaskAPI(url, task.id)

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
      links: {tasks: url, me: meUrl},
    } = getState()

    const user = await getMe(meUrl)
    const tasks = await getUserTasks(url, user)

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
    const {
      orgs,
      links: {tasks: url},
    } = getState()

    const task = await getTask(url, id)
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
      links: {tasks: url},
      tasks: {currentScript: script, currentTask: task},
    } = getState()

    await updateTaskFlux(url, task.id, script)

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
    dispatch(clearTaskOptions())
    dispatch(goToTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskNotCreated(e.headers['x-influx-error'])))
  }
}

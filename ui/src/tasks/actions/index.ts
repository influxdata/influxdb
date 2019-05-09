// Libraries
import {push, goBack} from 'react-router-redux'
import _ from 'lodash'

// APIs
import {LogEvent, ITask as Task} from '@influxdata/influx'
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
  taskGetFailed,
} from 'src/shared/copy/notifications'
import {
  importTaskFailed,
  importTaskSucceeded,
} from 'src/shared/copy/notifications'
import {createTaskFromTemplate as createTaskFromTemplateAJAX} from 'src/templates/api'

// Actions
import {setExportTemplate} from 'src/templates/actions'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {AppState, Label} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'
import {ITaskTemplate} from '@influxdata/influx'
import {Run} from 'src/tasks/components/TaskRunsPage'

// Utils
import {getErrorMessage} from 'src/utils/api'
import {insertPreambleInScript} from 'src/shared/utils/insertPreambleInScript'
import {TaskOptionKeys, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {taskToTemplate} from 'src/shared/utils/resourceToTemplate'
import {isLimitError} from 'src/cloud/utils/limits'
import {checkTaskLimits} from 'src/cloud/actions/limits'

export type Action =
  | SetNewScript
  | SetTasks
  | SetSearchTerm
  | SetCurrentScript
  | SetCurrentTask
  | SetShowInactive
  | SetTaskInterval
  | SetTaskCron
  | ClearTask
  | SetTaskOption
  | SetAllTaskOptions
  | SetRuns
  | SetLogs
  | UpdateTask
  | SetTaskStatus

type GetStateFunc = () => AppState

export interface SetAllTaskOptions {
  type: 'SET_ALL_TASK_OPTIONS'
  payload: Task
}

export interface SetTaskStatus {
  type: 'SET_TASKS_STATUS'
  payload: {
    status: RemoteDataState
  }
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

export interface SetTaskOption {
  type: 'SET_TASK_OPTION'
  payload: {
    key: TaskOptionKeys
    value: string
  }
}

export interface SetRuns {
  type: 'SET_RUNS'
  payload: {
    runs: Run[]
    runStatus: RemoteDataState
  }
}

export interface SetLogs {
  type: 'SET_LOGS'
  payload: {
    logs: LogEvent[]
  }
}

export interface UpdateTask {
  type: 'UPDATE_TASK'
  payload: {
    task: Task
  }
}

export const setTaskOption = (taskOption: {
  key: TaskOptionKeys
  value: string
}): SetTaskOption => ({
  type: 'SET_TASK_OPTION',
  payload: taskOption,
})

export const setTasksStatus = (status: RemoteDataState): SetTaskStatus => ({
  type: 'SET_TASKS_STATUS',
  payload: {status},
})

export const setAllTaskOptions = (task: Task): SetAllTaskOptions => ({
  type: 'SET_ALL_TASK_OPTIONS',
  payload: task,
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

export const setRuns = (runs: Run[], runStatus: RemoteDataState): SetRuns => ({
  type: 'SET_RUNS',
  payload: {runs, runStatus},
})

export const setLogs = (logs: LogEvent[]): SetLogs => ({
  type: 'SET_LOGS',
  payload: {logs},
})

export const updateTask = (task: Task): UpdateTask => ({
  type: 'UPDATE_TASK',
  payload: {task},
})

// Thunks
export const getTasks = () => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    dispatch(setTasksStatus(RemoteDataState.Loading))
    const {
      orgs: {org},
    } = getState()

    const tasks = await client.tasks.getAll(org.id)

    dispatch(setTasks(tasks))
    dispatch(setTasksStatus(RemoteDataState.Done))
  } catch (e) {
    dispatch(setTasksStatus(RemoteDataState.Error))
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(tasksFetchFailed(message)))
  }
}

export const addTaskLabelsAsync = (taskID: string, labels: Label[]) => async (
  dispatch
): Promise<void> => {
  try {
    await client.tasks.addLabels(taskID, labels.map(l => l.id))
    const task = await client.tasks.get(taskID)

    dispatch(updateTask(task))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addTaskLabelFailed()))
  }
}

export const removeTaskLabelsAsync = (
  taskID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.tasks.removeLabels(taskID, labels.map(l => l.id))
    const task = await client.tasks.get(taskID)

    dispatch(updateTask(task))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removeTaskLabelFailed()))
  }
}

export const updateTaskStatus = (task: Task) => async dispatch => {
  try {
    await client.tasks.updateStatus(task.id, task.status)

    dispatch(getTasks())
    dispatch(notify(taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskUpdateFailed(message)))
  }
}

export const updateTaskName = (task: Task) => async dispatch => {
  try {
    await client.tasks.update(task.id, task)

    dispatch(getTasks())
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

    dispatch(getTasks())
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
    dispatch(getTasks())
    dispatch(checkTaskLimits())
  } catch (error) {
    console.error(error)
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('tasks')))
    } else {
      const message = getErrorMessage(error)
      dispatch(notify(taskCloneFailed(task.name, message)))
    }
  }
}

export const selectTaskByID = (id: string) => async (
  dispatch
): Promise<void> => {
  try {
    const task = await client.tasks.get(id)

    dispatch(setCurrentTask(task))
  } catch (e) {
    console.error(e)
    dispatch(goToTasks())
    const message = getErrorMessage(e)
    dispatch(notify(taskNotFound(message)))
  }
}

export const selectTask = (task: Task) => async (
  dispatch,
  getState: GetStateFunc
) => {
  const {
    orgs: {org},
  } = getState()

  dispatch(push(`/orgs/${org.id}/tasks/${task.id}`))
}

export const goToTasks = () => async (dispatch, getState: GetStateFunc) => {
  const {
    orgs: {org},
  } = getState()

  dispatch(push(`/orgs/${org.id}/tasks`))
}

export const cancel = () => async dispatch => {
  dispatch(setCurrentTask(null))
  dispatch(goBack())
}

export const updateScript = () => async (dispatch, getState: GetStateFunc) => {
  try {
    const {
      tasks: {currentScript: script, currentTask: task, taskOptions},
    } = getState()

    const updatedTask: Partial<Task> & {name: string; flux: string} = {
      flux: script,
      name: taskOptions.name,
      offset: taskOptions.offset,
    }

    if (taskOptions.taskScheduleType === TaskSchedule.interval) {
      updatedTask.every = taskOptions.interval
      updatedTask.cron = null
    } else {
      updatedTask.cron = taskOptions.cron
      updatedTask.every = null
    }

    await client.tasks.update(task.id, updatedTask)

    dispatch(goToTasks())
    dispatch(setCurrentTask(null))
    dispatch(notify(taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(taskUpdateFailed(message)))
  }
}

export const saveNewScript = (script: string, preamble: string) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const fluxScript = await insertPreambleInScript(script, preamble)

    const {
      orgs: {org},
    } = getState()

    await client.tasks.createByOrgID(org.id, fluxScript)

    dispatch(setNewScript(''))
    dispatch(clearTask())
    dispatch(getTasks())
    dispatch(goToTasks())
    dispatch(notify(taskCreatedSuccess()))
    dispatch(checkTaskLimits())
  } catch (error) {
    console.error(error)
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('tasks')))
    } else {
      const message = getErrorMessage(error)
      dispatch(notify(taskNotCreated(message)))
    }
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

    const {
      orgs: {org},
    } = await getState()

    await client.tasks.createByOrgID(org.id, script)

    dispatch(getTasks())

    dispatch(notify(taskImportSuccess()))
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(taskImportFailed(message)))
  }
}

export const getRuns = (taskID: string) => async (dispatch): Promise<void> => {
  try {
    dispatch(setRuns([], RemoteDataState.Loading))

    const [runs] = await Promise.all([
      client.tasks.getRunsByTaskID(taskID),
      dispatch(selectTaskByID(taskID)),
    ])

    const runsWithDuration = runs.map(run => {
      const finished = new Date(run.finishedAt)
      const started = new Date(run.startedAt)

      return {
        ...run,
        duration: `${runDuration(finished, started)}`,
      }
    })

    dispatch(setRuns(runsWithDuration, RemoteDataState.Done))
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(taskGetFailed(message)))
    dispatch(setRuns([], RemoteDataState.Error))
  }
}

export const runTask = (taskID: string) => async dispatch => {
  try {
    await client.tasks.startRunByTaskID(taskID)
    dispatch(notify(taskRunSuccess()))
  } catch (error) {
    const message = getErrorMessage(error)
    dispatch(notify(copy.taskRunFailed(message)))
    console.error(error)
  }
}

export const getLogs = (taskID: string, runID: string) => async (
  dispatch
): Promise<void> => {
  try {
    const logs = await client.tasks.getLogEventsByRunID(taskID, runID)
    dispatch(setLogs(logs))
  } catch (error) {
    console.error(error)
    dispatch(setLogs([]))
  }
}

export const convertToTemplate = (taskID: string) => async (
  dispatch
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))
    const task = await client.tasks.get(taskID)
    const taskTemplate = taskToTemplate(task)

    dispatch(setExportTemplate(RemoteDataState.Done, taskTemplate))
  } catch (error) {
    dispatch(setExportTemplate(RemoteDataState.Error))
    dispatch(notify(copy.createTemplateFailed(error)))
  }
}

export const createTaskFromTemplate = (template: ITaskTemplate) => async (
  dispatch,
  getState: GetStateFunc
): Promise<void> => {
  try {
    const {
      orgs: {org},
    } = await getState()

    await createTaskFromTemplateAJAX(template, org.id)

    dispatch(getTasks())
    dispatch(notify(importTaskSucceeded()))
    dispatch(checkTaskLimits())
  } catch (error) {
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('tasks')))
    } else {
      dispatch(notify(importTaskFailed(error)))
    }
  }
}

export const runDuration = (finishedAt: Date, startedAt: Date): string => {
  let timeTag = 'seconds'

  if (isNaN(finishedAt.getTime()) || isNaN(startedAt.getTime())) {
    return ''
  }
  let diff = (finishedAt.getTime() - startedAt.getTime()) / 1000

  if (diff > 60) {
    diff = Math.round(diff / 60)
    timeTag = 'minutes'
  }

  return diff + ' ' + timeTag
}

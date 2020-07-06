// Libraries
import {push, goBack, RouterAction} from 'connected-react-router'
import {Dispatch} from 'react'
import {normalize} from 'normalizr'

// APIs
import * as api from 'src/client'
import {createTaskFromTemplate as createTaskFromTemplateAJAX} from 'src/templates/api'

// Schemas
import {taskSchema, arrayOfTasks} from 'src/schemas/tasks'

// Actions
import {setExportTemplate} from 'src/templates/actions/creators'
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  addTask,
  setTasks,
  editTask,
  setCurrentTask,
  setAllTaskOptions,
  setRuns,
  setLogs,
  clearTask,
  removeTask,
  setNewScript,
  clearCurrentTask,
  Action as TaskAction,
} from 'src/tasks/actions/creators'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {
  Label,
  TaskTemplate,
  Task,
  GetState,
  TaskSchedule,
  RemoteDataState,
  TaskEntities,
  ResourceType,
} from 'src/types'

// Utils
import {getErrorMessage} from 'src/utils/api'
import {insertPreambleInScript} from 'src/shared/utils/insertPreambleInScript'
import {taskToTemplate} from 'src/shared/utils/resourceToTemplate'
import {isLimitError} from 'src/cloud/utils/limits'
import {checkTaskLimits} from 'src/cloud/actions/limits'
import {getOrg} from 'src/organizations/selectors'
import {getStatus} from 'src/resources/selectors'

type Action = TaskAction | ExternalActions | ReturnType<typeof getTasks>
type ExternalActions = NotifyAction | ReturnType<typeof checkTaskLimits>

// Thunks
export const getTasks = () => async (
  dispatch: Dispatch<TaskAction | NotifyAction>,
  getState: GetState
): Promise<void> => {
  try {
    const state = getState()
    if (getStatus(state, ResourceType.Tasks) === RemoteDataState.NotStarted) {
      dispatch(setTasks(RemoteDataState.Loading))
    }

    const org = getOrg(state)

    const resp = await api.getTasks({query: {orgID: org.id}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const tasks = normalize<Task, TaskEntities, string[]>(
      resp.data.tasks,
      arrayOfTasks
    )

    dispatch(setTasks(RemoteDataState.Done, tasks))
  } catch (error) {
    dispatch(setTasks(RemoteDataState.Error))
    const message = getErrorMessage(error)
    console.error(error)
    dispatch(notify(copy.tasksFetchFailed(message)))
  }
}

export const addTaskLabel = (taskID: string, label: Label) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const postResp = await api.postTasksLabel({
      taskID,
      data: {labelID: label.id},
    })

    if (postResp.status !== 201) {
      throw new Error(postResp.data.message)
    }

    const resp = await api.getTask({taskID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const task = normalize<Task, TaskEntities, string>(resp.data, taskSchema)

    dispatch(editTask(task))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addTaskLabelFailed()))
  }
}

export const deleteTaskLabel = (taskID: string, label: Label) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const deleteResp = await api.deleteTasksLabel({taskID, labelID: label.id})
    if (deleteResp.status !== 204) {
      throw new Error(deleteResp.data.message)
    }

    const resp = await api.getTask({taskID})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const task = normalize<Task, TaskEntities, string>(resp.data, taskSchema)

    dispatch(editTask(task))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removeTaskLabelFailed()))
  }
}

export const updateTaskStatus = (task: Task) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.patchTask({
      taskID: task.id,
      data: {status: task.status},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const normTask = normalize<Task, TaskEntities, string>(
      resp.data,
      taskSchema
    )

    dispatch(editTask(normTask))
    dispatch(notify(copy.taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(copy.taskUpdateFailed(message)))
  }
}

export const updateTaskName = (name: string, taskID: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.patchTask({taskID, data: {name}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const normTask = normalize<Task, TaskEntities, string>(
      resp.data,
      taskSchema
    )

    dispatch(editTask(normTask))
    dispatch(notify(copy.taskUpdateSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(copy.taskUpdateFailed(message)))
  }
}

export const deleteTask = (taskID: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.deleteTask({taskID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeTask(taskID))
    dispatch(notify(copy.taskDeleteSuccess()))
  } catch (e) {
    console.error(e)
    const message = getErrorMessage(e)
    dispatch(notify(copy.taskDeleteFailed(message)))
  }
}

export const cloneTask = (task: Task) => async (dispatch: Dispatch<Action>) => {
  try {
    const resp = await api.getTask({taskID: task.id})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const newTask = await api.postTask({data: resp.data})

    if (newTask.status !== 201) {
      throw new Error(newTask.data.message)
    }

    const normTask = normalize<Task, TaskEntities, string>(
      resp.data,
      taskSchema
    )

    dispatch(notify(copy.taskCloneSuccess(task.name)))
    dispatch(addTask(normTask))
    dispatch(checkTaskLimits())
  } catch (error) {
    console.error(error)
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('tasks')))
    } else {
      const message = getErrorMessage(error)
      dispatch(notify(copy.taskCloneFailed(task.name, message)))
    }
  }
}

export const selectTaskByID = (id: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const resp = await api.getTask({taskID: id})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const task = normalize<Task, TaskEntities, string>(resp.data, taskSchema)

    dispatch(setCurrentTask(task))
  } catch (error) {
    console.error(error)
    dispatch(goToTasks())
    const message = getErrorMessage(error)
    dispatch(notify(copy.taskNotFound(message)))
  }
}

export const setAllTaskOptionsByID = (taskID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const resp = await api.getTask({taskID})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const task = normalize<Task, TaskEntities, string>(resp.data, taskSchema)

    dispatch(setAllTaskOptions(task))
  } catch (error) {
    console.error(error)
    dispatch(goToTasks())
    const message = getErrorMessage(error)
    dispatch(notify(copy.taskNotFound(message)))
  }
}

export const goToTasks = () => (
  dispatch: Dispatch<Action | RouterAction>,
  getState: GetState
) => {
  const org = getOrg(getState())

  dispatch(push(`/orgs/${org.id}/tasks`))
}

export const cancel = () => (dispatch: Dispatch<Action | RouterAction>) => {
  dispatch(clearCurrentTask())
  dispatch(goBack())
}

export const updateScript = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    const {
      tasks: {currentScript: script, currentTask: task, taskOptions},
    } = state.resources

    const updatedTask: Partial<Task> & {
      name: string
      flux: string
      token: string
    } = {
      flux: script,
      name: taskOptions.name,
      offset: taskOptions.offset,
      token: null,
    }

    if (taskOptions.taskScheduleType === TaskSchedule.interval) {
      updatedTask.every = taskOptions.interval
      updatedTask.cron = null
    } else {
      updatedTask.cron = taskOptions.cron
      updatedTask.every = null
    }

    const resp = await api.patchTask({taskID: task.id, data: updatedTask})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(goToTasks())
    dispatch(clearCurrentTask())
    dispatch(notify(copy.taskUpdateSuccess()))
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(copy.taskUpdateFailed(message)))
  }
}

export const saveNewScript = (script: string, preamble: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  try {
    const fluxScript = await insertPreambleInScript(script, preamble)
    const org = getOrg(getState())
    const resp = await api.postTask({data: {orgID: org.id, flux: fluxScript}})
    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(setNewScript(''))
    dispatch(clearTask())
    dispatch(goToTasks())
    dispatch(notify(copy.taskCreatedSuccess()))
    dispatch(checkTaskLimits())
  } catch (error) {
    console.error(error)
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('tasks')))
    } else {
      const message = getErrorMessage(error)
      dispatch(notify(copy.taskNotCreated(message)))
    }
  }
}

export const getRuns = (taskID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    dispatch(setRuns([], RemoteDataState.Loading))
    dispatch(selectTaskByID(taskID))
    const resp = await api.getTasksRuns({taskID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const runsWithDuration = resp.data.runs.map(run => {
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
    dispatch(notify(copy.taskGetFailed(message)))
    dispatch(setRuns([], RemoteDataState.Error))
  }
}

export const runTask = (taskID: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.postTasksRun({taskID})
    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(notify(copy.taskRunSuccess()))
  } catch (error) {
    const message = getErrorMessage(error)
    dispatch(notify(copy.taskRunFailed(message)))
    console.error(error)
  }
}

export const getLogs = (taskID: string, runID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const resp = await api.getTasksRunsLogs({taskID, runID})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }
    dispatch(setLogs(resp.data.events))
  } catch (error) {
    console.error(error)
    dispatch(setLogs([]))
  }
}

export const convertToTemplate = (taskID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))
    const resp = await api.getTask({taskID})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const {entities, result} = normalize<Task, TaskEntities, string>(
      resp.data,
      taskSchema
    )

    const taskTemplate = taskToTemplate(getState(), entities.tasks[result])

    dispatch(setExportTemplate(RemoteDataState.Done, taskTemplate))
  } catch (error) {
    dispatch(setExportTemplate(RemoteDataState.Error))
    dispatch(notify(copy.createTemplateFailed(error)))
  }
}

export const createTaskFromTemplate = (template: TaskTemplate) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  try {
    const org = getOrg(getState())

    await createTaskFromTemplateAJAX(template, org.id)

    dispatch(getTasks())
    dispatch(notify(copy.importTaskSucceeded()))
    dispatch(checkTaskLimits())
  } catch (error) {
    if (isLimitError(error)) {
      dispatch(notify(copy.resourceLimitReached('tasks')))
    } else {
      dispatch(notify(copy.importTaskFailed(error)))
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

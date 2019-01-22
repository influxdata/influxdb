import _ from 'lodash'

// Utils
import {addLabelDefaults} from 'src/shared/utils/labels'

// Types
import {Task} from 'src/api'
import {Label} from 'src/types/v2'
import {taskAPI} from 'src/utils/api'

export const submitNewTask = async (
  orgID: string,
  flux: string
): Promise<Task> => {
  const {data} = await taskAPI.tasksPost({orgID, flux})

  return data
}

export const updateTaskFlux = async (id, task: Task): Promise<Task> => {
  const {data} = await taskAPI.tasksTaskIDPatch(id, task)

  return data
}

export const updateTaskStatus = async (
  id: string,
  status: Task.StatusEnum
): Promise<Task> => {
  const {data: task} = await taskAPI.tasksTaskIDGet(id)
  const {data} = await taskAPI.tasksTaskIDPatch(id, {...task, status})

  return data
}

export const getUserTasks = async (user): Promise<Task[]> => {
  const after = ''
  const {data} = await taskAPI.tasksGet(after, user.id)

  return data.tasks
}

export const getTask = async (id): Promise<Task> => {
  const {data} = await taskAPI.tasksTaskIDGet(id)

  return data
}

export const deleteTask = (taskID: string) => {
  return taskAPI.tasksTaskIDDelete(taskID)
}

export const addTaskLabels = async (
  taskID: string,
  labels: Label[]
): Promise<Label[]> => {
  await Promise.all(
    labels.map(async label => {
      await taskAPI.tasksTaskIDLabelsPost(taskID, {labelID: label.id})
    })
  )

  const {data} = await taskAPI.tasksTaskIDLabelsGet(taskID)

  return data.labels.map(addLabelDefaults)
}

export const removeTaskLabels = async (
  taskID: string,
  labels: Label[]
): Promise<void> => {
  await Promise.all(
    labels.map(async label => {
      const id = _.get(label, 'id', '')
      await taskAPI.tasksTaskIDLabelsLabelIDDelete(taskID, id)
    })
  )
}

import {Task} from 'src/api'
import {taskAPI} from 'src/utils/api'

export const submitNewTask = async (
  organizationId: string,
  flux: string
): Promise<Task> => {
  const {data} = await taskAPI.tasksPost({organizationId, flux})

  return data
}

export const updateTaskFlux = async (id, flux: string): Promise<Task> => {
  const {data} = await taskAPI.tasksTaskIDPatch(id, {flux})

  return data
}

export const updateTaskStatus = async (
  id: string,
  status: Task.StatusEnum
): Promise<Task> => {
  const {data} = await taskAPI.tasksTaskIDPatch(id, {status})

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

import {Task, TasksApi} from 'src/api'

const getBasePath = () => {
  const host = window.location.hostname
  const port = window.location.port
  const protocol = window.location.protocol

  return `${protocol}//${host}:${port}/api/v2`
}

const createTaskAPI = () => {
  return new TasksApi({basePath: getBasePath()})
}

export const submitNewTask = async (
  organizationId: string,
  flux: string
): Promise<Task> => {
  const api = createTaskAPI()
  const {data} = await api.tasksPost({organizationId, flux})

  return data
}

export const updateTaskFlux = async (id, flux: string): Promise<Task> => {
  const api = createTaskAPI()
  const {data} = await api.tasksTaskIDPatch(id, {flux})

  return data
}

export const updateTaskStatus = async (
  id: string,
  status: Task.StatusEnum
): Promise<Task> => {
  const api = createTaskAPI()
  const {data} = await api.tasksTaskIDPatch(id, {status})

  return data
}

export const getUserTasks = async (user): Promise<Task[]> => {
  const api = createTaskAPI()
  const after = ''
  const {data} = await api.tasksGet(after, user.id)

  return data.tasks
}

export const getTask = async (id): Promise<Task> => {
  const api = createTaskAPI()
  const {data} = await api.tasksTaskIDGet(id)

  return data.task
}

export const deleteTask = (taskID: string) => {
  const api = createTaskAPI()

  return api.tasksTaskIDDelete(taskID)
}

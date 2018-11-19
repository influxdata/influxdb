import AJAX from 'src/utils/ajax'

import {Task, TasksApi} from 'src/api'

const getBasePath = () => {
  const host = window.location.hostname
  const port = window.location.port
  const protocol = window.location.protocol

  return `${protocol}//${host}:${port}/api/v2`
}

export const submitNewTask = async (
  organizationId: string,
  flux: string
): Promise<Task> => {
  const api = new TasksApi({basePath: getBasePath()})
  const {data} = await api.tasksPost({organizationId, flux})

  return data
}

export const updateTaskFlux = async (url, id, flux: string): Promise<Task> => {
  const completeUrl = `${url}/${id}`
  const request = {
    flux,
  }

  const {data} = await AJAX({url: completeUrl, data: request, method: 'PATCH'})

  return data
}

export const updateTaskStatus = async (
  url: string,
  id: string,
  status: string
): Promise<Task> => {
  const completeUrl = `${url}/${id}`
  const request = {
    status,
  }

  const {data} = await AJAX({url: completeUrl, data: request, method: 'PATCH'})

  return data
}

export const getUserTasks = async (url, user): Promise<Task[]> => {
  const completeUrl = `${url}?user=${user.id}`

  const {
    data: {tasks},
  } = await AJAX({url: completeUrl})
  return tasks
}

export const getTask = async (url, id): Promise<Task> => {
  const completeUrl = `${url}/${id}`
  const {
    data: {task},
  } = await AJAX({url: completeUrl})

  return task
}

export const deleteTask = (url: string, taskID: string) => {
  const completeUrl = `${url}/${taskID}`

  return AJAX({url: completeUrl, method: 'DELETE'})
}

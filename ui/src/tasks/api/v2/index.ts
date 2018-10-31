import AJAX from 'src/utils/ajax'
import {Task} from 'src/types/v2/tasks'

export const submitNewTask = async (
  url,
  owner,
  org,
  flux: string
): Promise<Task> => {
  const request = {
    flux,
    organizationId: org.id,
    status: 'active',
    owner,
  }

  const {data} = await AJAX({url, data: request, method: 'POST'})

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

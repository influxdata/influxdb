import {push} from 'react-router-redux'

import {Task} from 'src/types/v2/tasks'
import {
  submitNewTask,
  getUserTasks,
  deleteTask as deleteTaskAPI,
} from 'src/tasks/api/v2'
import {getMe} from 'src/shared/apis/v2/user'
import {getOrganizations} from 'src/shared/apis/v2/organization'
import {notify} from 'src/shared/actions/notifications'
import {
  taskNotCreated,
  tasksFetchFailed,
  taskDeleteFailed,
} from 'src/shared/copy/v2/notifications'

export type Action = SetNewScript | SetTasks

export enum ActionTypes {
  SetNewScript = 'SET_NEW_SCRIPT',
  SetTasks = 'SET_TASKS',
}

export interface SetNewScript {
  type: ActionTypes.SetNewScript
  payload: {
    script: string
  }
}

export interface SetTasks {
  type: ActionTypes.SetTasks
  payload: {
    tasks: Task[]
  }
}

export const setTasks = (tasks: Task[]): SetTasks => ({
  type: ActionTypes.SetTasks,
  payload: {tasks},
})

export const setNewScript = (script: string): SetNewScript => ({
  type: ActionTypes.SetNewScript,
  payload: {script},
})

export const deleteTask = (task: Task) => async (dispatch, getState) => {
  try {
    const {
      links: {tasks: url},
    } = await getState()

    await deleteTaskAPI(url, task.id)

    dispatch(populateTasks())
  } catch (e) {
    console.error(e)
    dispatch(notify(taskDeleteFailed()))
  }
}

export const populateTasks = () => async (
  dispatch,
  getState
): Promise<void> => {
  try {
    const {
      orgs,
      links: {tasks: url, me: meUrl},
    } = await getState()

    const user = await getMe(meUrl)
    const tasks = await getUserTasks(url, user)

    const mappedTasks = tasks.map(task => {
      return {
        ...task,
        organization: orgs.find(org => org.id === task.organizationId),
      }
    })

    dispatch(setTasks(mappedTasks))
  } catch (e) {
    console.error(e)
    dispatch(notify(tasksFetchFailed()))
  }
}

export const saveNewScript = () => async (
  dispatch,
  getState
): Promise<void> => {
  try {
    const {
      orgs,
      links: {tasks: url, me: meUrl},
      tasks: {newScript: script},
    } = await getState()

    const user = await getMe(meUrl)

    await submitNewTask(url, user, orgs[0], script)

    dispatch(setNewScript(''))
    dispatch(push('/tasks'))
  } catch (e) {
    console.error(e)
    dispatch(notify(taskNotCreated()))
  }
}

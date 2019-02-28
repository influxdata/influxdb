import {Task, Organization} from 'src/types/v2'

import {client} from 'src/utils/api'

export enum ActionTypes {
  GetTasks = 'GET_TASKS',
  PopulateTasks = 'POPULATE_TASKS',
}

export type Actions = PopulateTasks

export interface PopulateTasks {
  type: ActionTypes.PopulateTasks
  payload: {tasks: Task[]}
}

export const populateTasks = (tasks: Task[]): PopulateTasks => ({
  type: ActionTypes.PopulateTasks,
  payload: {tasks},
})

export const getTasks = (org: Organization) => async dispatch => {
  const tasks = await client.tasks.getAllByOrg(org.name)
  const organization = await client.organizations.get(org.id)
  const tasksWithOrg = tasks.map(t => ({...t, organization})) as Task[]

  dispatch(populateTasks(tasksWithOrg))
}

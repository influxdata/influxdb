import {Organization} from 'src/types/v2'
import {ScraperTargetRequest, Task} from '@influxdata/influx'

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
  console.log(tasks)
  dispatch(populateTasks(tasks))
}

export const createScraper = (scraper: ScraperTargetRequest) => async () => {
  await client.scrapers.create(scraper)
}

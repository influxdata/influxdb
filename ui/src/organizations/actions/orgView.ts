import _ from 'lodash'
import {Organization} from 'src/types/v2'
import {ScraperTargetRequest, Task} from '@influxdata/influx'
import {client} from 'src/utils/api'
import {Template} from 'src/shared/utils/resourceToTemplate'

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

export const createTaskFromTemplate = (
  template: Template,
  orgID: string
) => async dispatch => {
  // try catch
  client.tasks.createFromTemplate(template, orgID)

  dispatch(getTasks(orgID))
}

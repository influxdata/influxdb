import _ from 'lodash'

import {
  ScraperTargetRequest,
  ITask as Task,
  ITaskTemplate,
} from '@influxdata/influx'

// API
import {client} from 'src/utils/api'

// Actions
import {UpdateTask} from 'src/tasks/actions/v2'

export enum ActionTypes {
  GetTasks = 'GET_TASKS',
  PopulateTasks = 'POPULATE_TASKS',
  UpdateTask = 'UPDATE_TASK',
}

export type Actions = UpdateTask | PopulateTasks

export interface PopulateTasks {
  type: ActionTypes.PopulateTasks
  payload: {tasks: Task[]}
}

export const populateTasks = (tasks: Task[]): PopulateTasks => ({
  type: ActionTypes.PopulateTasks,
  payload: {tasks},
})

export const getTasks = (orgID: string) => async dispatch => {
  const tasks = await client.tasks.getAllByOrgID(orgID)
  dispatch(populateTasks(tasks))
}

export const createScraper = (scraper: ScraperTargetRequest) => async () => {
  await client.scrapers.create(scraper)
}

export const createTaskFromTemplate = (
  template: ITaskTemplate,
  orgID: string
) => async () => {
  await client.tasks.createFromTemplate(template, orgID)
}

import _ from 'lodash'

import {
  ScraperTargetRequest,
  ITask as Task,
  ITaskTemplate,
} from '@influxdata/influx'
import {Dashboard} from 'src/types/v2'

// API
import {client} from 'src/utils/api'
import {getDashboardsByOrgID} from 'src/dashboards/apis/v2/index'

export enum ActionTypes {
  GetTasks = 'GET_TASKS',
  PopulateTasks = 'POPULATE_TASKS',
  getDashboards = 'GET_DASHBOARDS',
  PopulateDashboards = 'POPULATE_DASHBOARDS',
}

export type Actions = PopulateTasks | PopulateDashboards

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

export interface PopulateDashboards {
  type: ActionTypes.PopulateDashboards
  payload: {dashboards: Dashboard[]}
}

export const populateDashboards = (
  dashboards: Dashboard[]
): PopulateDashboards => ({
  type: ActionTypes.PopulateDashboards,
  payload: {dashboards},
})

export const getDashboards = (orgID: string) => async dispatch => {
  const dashboards = await getDashboardsByOrgID(orgID)
  dispatch(populateDashboards(dashboards))
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

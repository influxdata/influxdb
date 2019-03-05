import _ from 'lodash'

import {notify} from 'src/shared/actions/notifications'

import {ScraperTargetRequest, Task, ITaskTemplate} from '@influxdata/influx'

import {
  importTaskSucceeded,
  importTaskFailed,
} from 'src/shared/copy/notifications'

// API
import {client} from 'src/utils/api'

// Actions
import {
  Action as TaskLabelsAction,
  addTaskLabelsFactoryAsync,
} from 'src/tasks/actions/v2/labels'

// Types
import {Organization, AppState} from 'src/types/v2'

export enum ActionTypes {
  GetTasks = 'GET_TASKS',
  PopulateTasks = 'POPULATE_TASKS',
  AddTaskLabels = 'ADD_TASK_LABELS',
  RemoveTaskLabels = 'REMOVE_TASK_LABELS',
}

export type Actions = TaskLabelsAction | PopulateTasks

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
) => async dispatch => {
  try {
    await client.tasks.createFromTemplate(template, orgID)

    dispatch(notify(importTaskSucceeded()))
  } catch (error) {
    dispatch(notify(importTaskFailed(error)))
  }
}
export const addTaskLabelsAsync = addTaskLabelsFactoryAsync(
  (state: AppState) => state.orgView.tasks
)

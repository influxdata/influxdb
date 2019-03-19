import _ from 'lodash'

//Types
import {TemplateSummary, DocumentCreate} from '@influxdata/influx'

// API
import {client} from 'src/utils/api'

export enum ActionTypes {
  getTemplateSummariesForOrg = 'GET_TEMPLATE_SUMMARIES_FOR_ORG',
  PopulateTemplateSummaries = 'POPULATE_TEMPLATE_SUMMARIES',
  SetStatusToLoading = 'SET_STATUS_TO_LOADING',
}

export type Actions = PopulateTemplateSummaries | SetStatusToLoading

export interface PopulateTemplateSummaries {
  type: ActionTypes.PopulateTemplateSummaries
  payload: {items: TemplateSummary[]}
}

export const populateTemplateSummaries = (
  items: TemplateSummary[]
): PopulateTemplateSummaries => ({
  type: ActionTypes.PopulateTemplateSummaries,
  payload: {items},
})

export interface SetStatusToLoading {
  type: ActionTypes.SetStatusToLoading
  payload: {}
}

export const setStatusToLoading = (): SetStatusToLoading => ({
  type: ActionTypes.SetStatusToLoading,
  payload: {},
})

export const getTemplatesForOrg = (orgName: string) => async dispatch => {
  dispatch(setStatusToLoading)
  const items = await client.templates.getAll(orgName)
  dispatch(populateTemplateSummaries(items))
}

export const createTemplate = async (template: DocumentCreate) => {
  await client.templates.create(template)
}

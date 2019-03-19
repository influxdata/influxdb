import _ from 'lodash'

//Types
import {TemplateSummary, DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

// API
import {client} from 'src/utils/api'

export enum ActionTypes {
  GetTemplateSummariesForOrg = 'GET_TEMPLATE_SUMMARIES_FOR_ORG',
  PopulateTemplateSummaries = 'POPULATE_TEMPLATE_SUMMARIES',
  SetTemplatesStatus = 'SET_TEMPLATES_STATUS',
}

export type Actions = PopulateTemplateSummaries | SetTemplatesStatus

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

export interface SetTemplatesStatus {
  type: ActionTypes.SetTemplatesStatus
  payload: {status: RemoteDataState}
}

export const setTemplatesStatus = (
  status: RemoteDataState
): SetTemplatesStatus => ({
  type: ActionTypes.SetTemplatesStatus,
  payload: {status},
})

export const getTemplatesForOrg = (orgName: string) => async dispatch => {
  dispatch(setTemplatesStatus(RemoteDataState.Loading))
  const items = await client.templates.getAll(orgName)
  dispatch(populateTemplateSummaries(items))
}

export const createTemplate = async (template: DocumentCreate) => {
  await client.templates.create(template)
}

import _ from 'lodash'

// Utils
import {templateToExport} from 'src/shared/utils/resourceToTemplate'

// Types
import {TemplateSummary, DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'

// constants
import * as copy from 'src/shared/copy/notifications'

// API
import {client} from 'src/utils/api'

export enum ActionTypes {
  GetTemplateSummariesForOrg = 'GET_TEMPLATE_SUMMARIES_FOR_ORG',
  PopulateTemplateSummaries = 'POPULATE_TEMPLATE_SUMMARIES',
  SetTemplatesStatus = 'SET_TEMPLATES_STATUS',
  SetExportTemplate = 'SET_EXPORT_TEMPLATE',
}

export type Actions =
  | PopulateTemplateSummaries
  | SetTemplatesStatus
  | SetExportTemplate

export interface PopulateTemplateSummaries {
  type: ActionTypes.PopulateTemplateSummaries
  payload: {items: TemplateSummary[]; status: RemoteDataState}
}

export const populateTemplateSummaries = (
  items: TemplateSummary[]
): PopulateTemplateSummaries => ({
  type: ActionTypes.PopulateTemplateSummaries,
  payload: {items, status: RemoteDataState.Done},
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

export interface SetExportTemplate {
  type: ActionTypes.SetExportTemplate
  payload: {status: RemoteDataState; item?: DocumentCreate}
}

export const setExportTemplate = (
  status: RemoteDataState,
  item?: DocumentCreate
): SetExportTemplate => ({
  type: ActionTypes.SetExportTemplate,
  payload: {status, item},
})

export const getTemplatesForOrg = (orgName: string) => async dispatch => {
  dispatch(setTemplatesStatus(RemoteDataState.Loading))
  const items = await client.templates.getAll(orgName)
  dispatch(populateTemplateSummaries(items))
}

export const createTemplate = async (template: DocumentCreate) => {
  await client.templates.create(template)
}

export const convertToTemplate = (id: string) => async (
  dispatch
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))

    const templateDocument = await client.templates.get(id)
    const template = templateToExport(templateDocument)

    dispatch(setExportTemplate(RemoteDataState.Done, template))
  } catch (error) {
    dispatch(setExportTemplate(RemoteDataState.Error))
    dispatch(notify(copy.createTemplateFailed(error)))
  }
}

export const clearExportTemplate = async () => async dispatch => {
  dispatch(setExportTemplate(RemoteDataState.NotStarted, null))
}

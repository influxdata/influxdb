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
  RemoveTemplateSummary = 'REMOVE_TEMPLATE_SUMMARY',
  AddTemplateSummary = 'ADD_TEMPLATE_SUMMARY',
  SetTemplateSummary = 'SET_TEMPLATE_SUMMARY',
}

export type Actions =
  | PopulateTemplateSummaries
  | SetTemplatesStatus
  | SetExportTemplate
  | RemoveTemplateSummary
  | AddTemplateSummary
  | SetTemplateSummary

export interface AddTemplateSummary {
  type: ActionTypes.AddTemplateSummary
  payload: {item: TemplateSummary}
}

export const addTemplateSummary = (
  item: TemplateSummary
): AddTemplateSummary => ({
  type: ActionTypes.AddTemplateSummary,
  payload: {item},
})

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
  payload: {status: RemoteDataState; item?: DocumentCreate; orgID: string}
}

export const setExportTemplate = (
  status: RemoteDataState,
  item?: DocumentCreate,
  orgID?: string
): SetExportTemplate => ({
  type: ActionTypes.SetExportTemplate,
  payload: {status, item, orgID},
})

interface RemoveTemplateSummary {
  type: ActionTypes.RemoveTemplateSummary
  payload: {templateID: string}
}

const removeTemplateSummary = (templateID: string): RemoveTemplateSummary => ({
  type: ActionTypes.RemoveTemplateSummary,
  payload: {templateID},
})

export const getTemplatesForOrg = (orgName: string) => async dispatch => {
  dispatch(setTemplatesStatus(RemoteDataState.Loading))
  const items = await client.templates.getAll(orgName)
  dispatch(populateTemplateSummaries(items))
}

export const createTemplate = (template: DocumentCreate) => async dispatch => {
  try {
    await client.templates.create(template)
    dispatch(notify(copy.importTemplateSucceeded()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.importTemplateFailed(e)))
  }
}

interface SetTemplateSummary {
  type: ActionTypes.SetTemplateSummary
  payload: {id: string; templateSummary: TemplateSummary}
}

export const setTemplateSummary = (
  id: string,
  templateSummary: TemplateSummary
): SetTemplateSummary => ({
  type: ActionTypes.SetTemplateSummary,
  payload: {id, templateSummary},
})

export const updateTemplate = (id: string, props: TemplateSummary) => async (
  dispatch
): Promise<void> => {
  try {
    const {meta} = await client.templates.update(id, props)

    dispatch(setTemplateSummary(id, {...props, meta}))
    dispatch(notify(copy.updateTemplateSucceeded()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.updateTemplateFailed(e)))
  }
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

export const clearExportTemplate = () => async dispatch => {
  dispatch(setExportTemplate(RemoteDataState.NotStarted, null))
}

export const deleteTemplate = (templateID: string) => async (
  dispatch
): Promise<void> => {
  try {
    await client.templates.delete(templateID)
    dispatch(removeTemplateSummary(templateID))
    dispatch(notify(copy.deleteTemplateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.deleteTemplateFailed(e)))
  }
}

export const cloneTemplate = (templateID: string, orgID: string) => async (
  dispatch
): Promise<void> => {
  try {
    const createdTemplate = await client.templates.clone(templateID, orgID)

    dispatch(
      addTemplateSummary({
        ...createdTemplate,
        labels: createdTemplate.labels || [],
      })
    )
    dispatch(notify(copy.cloneTemplateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.cloneTemplateFailed(e)))
  }
}

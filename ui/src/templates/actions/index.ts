// Utils
import {templateToExport} from 'src/shared/utils/resourceToTemplate'

import {staticTemplates} from 'src/templates/constants/defaultTemplates'

// Types
import {
  TemplateSummary,
  DocumentCreate,
  ITaskTemplate,
  TemplateType,
  ITemplate,
} from '@influxdata/influx'
import {
  RemoteDataState,
  GetState,
  DashboardTemplate,
  VariableTemplate,
  Template,
  Label,
} from 'src/types'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import * as copy from 'src/shared/copy/notifications'

// API
import {client} from 'src/utils/api'
import {createDashboardFromTemplate} from 'src/dashboards/actions'
import {createVariableFromTemplate} from 'src/variables/actions'
import {createTaskFromTemplate} from 'src/tasks/actions'

// Selectors
import {getOrg} from 'src/organizations/selectors'

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
  payload: {status: RemoteDataState; item?: DocumentCreate}
}

export const setExportTemplate = (
  status: RemoteDataState,
  item?: DocumentCreate
): SetExportTemplate => ({
  type: ActionTypes.SetExportTemplate,
  payload: {status, item},
})

interface RemoveTemplateSummary {
  type: ActionTypes.RemoveTemplateSummary
  payload: {templateID: string}
}

const removeTemplateSummary = (templateID: string): RemoveTemplateSummary => ({
  type: ActionTypes.RemoveTemplateSummary,
  payload: {templateID},
})

export const getTemplateByID = async (id: string) => {
  const template = (await client.templates.get(id)) as Template
  return template
}

export const getTemplates = () => async (dispatch, getState: GetState) => {
  const org = getOrg(getState())
  dispatch(setTemplatesStatus(RemoteDataState.Loading))
  const items = await client.templates.getAll(org.id)
  dispatch(populateTemplateSummaries(items))
}

export const createTemplate = (template: DocumentCreate) => async (
  dispatch,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())
    await client.templates.create({...template, orgID: org.id})
    dispatch(notify(copy.importTemplateSucceeded()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.importTemplateFailed(e)))
  }
}

export const createTemplateFromResource = (
  resource: DocumentCreate,
  resourceName: string
) => async (dispatch, getState: GetState) => {
  try {
    const org = getOrg(getState())
    await client.templates.create({...resource, orgID: org.id})
    dispatch(notify(copy.resourceSavedAsTemplate(resourceName)))
  } catch (e) {
    console.error(e)
    dispatch(copy.saveResourceAsTemplateFailed(resourceName, e))
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

export const clearExportTemplate = () => dispatch => {
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

export const cloneTemplate = (templateID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    const org = getOrg(getState())
    const createdTemplate = await client.templates.clone(templateID, org.id)

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

const createFromTemplate = template => dispatch => {
  const {
    content: {
      data: {type},
    },
  } = template

  try {
    switch (type) {
      case TemplateType.Dashboard:
        return dispatch(
          createDashboardFromTemplate(template as DashboardTemplate)
        )
      case TemplateType.Task:
        return dispatch(createTaskFromTemplate(template as ITaskTemplate))
      case TemplateType.Variable:
        return dispatch(
          createVariableFromTemplate(template as VariableTemplate)
        )
      default:
        throw new Error(`Cannot create template: ${type}`)
    }
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createResourceFromTemplateFailed(e)))
  }
}

export const createResourceFromStaticTemplate = (name: string) => dispatch => {
  const template = staticTemplates[name]
  dispatch(createFromTemplate(template))
}

export const createResourceFromTemplate = (templateID: string) => async (
  dispatch
): Promise<void> => {
  const template = await client.templates.get(templateID)
  dispatch(createFromTemplate(template))
}

export const addTemplateLabelsAsync = (
  templateID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.templates.addLabels(templateID, labels.map(l => l.id))
    const template = await client.templates.get(templateID)

    dispatch(setTemplateSummary(templateID, templateToSummary(template)))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addTemplateLabelFailed()))
  }
}

export const removeTemplateLabelsAsync = (
  templateID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.templates.removeLabels(templateID, labels.map(l => l.id))
    const template = await client.templates.get(templateID)

    dispatch(setTemplateSummary(templateID, templateToSummary(template)))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removeTemplateLabelFailed()))
  }
}

const templateToSummary = (template: ITemplate): TemplateSummary => ({
  id: template.id,
  meta: template.meta,
  labels: template.labels,
})

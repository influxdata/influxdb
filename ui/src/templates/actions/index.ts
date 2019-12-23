import _ from 'lodash'

// Utils
import {templateToExport} from 'src/shared/utils/resourceToTemplate'

import {staticTemplates} from 'src/templates/constants/defaultTemplates'

// Types
import {TemplateType} from '@influxdata/influx'
import {
  RemoteDataState,
  GetState,
  DashboardTemplate,
  VariableTemplate,
  TaskTemplate,
  Template,
  TemplateSummary,
} from 'src/types'
import {Document, DocumentCreate, Label} from 'src/client'
// Actions
import {notify} from 'src/shared/actions/notifications'

// constants
import * as copy from 'src/shared/copy/notifications'

// API
import {client} from 'src/utils/api'
import {createDashboardFromTemplate} from 'src/dashboards/actions'
import {createVariableFromTemplate} from 'src/variables/actions'
import {createTaskFromTemplate} from 'src/tasks/actions'
import * as api from 'src/client'

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
  try {
    const resp = await api.getDocumentsTemplate({templateID: id})
    if (resp.status !== 200) {
      throw new Error(
        'An error occurred trying to get the template based on the ID'
      )
    }
    return resp.data
  } catch (e) {
    console.error(e)
    return null
  }
}

export const getTemplates = () => async (dispatch, getState: GetState) => {
  const {
    orgs: {org},
  } = getState()
  dispatch(setTemplatesStatus(RemoteDataState.Loading))
  try {
    const resp = await api.getDocumentsTemplates({query: {orgID: org.id}})
    if (resp.status !== 200) {
      throw new Error("Couldn't get the templates for this org")
    }

    const items = resp.data as TemplateSummary[]

    dispatch(populateTemplateSummaries(items))
  } catch (e) {
    console.error(e)
  }
}

export const createTemplate = (template: DocumentCreate) => async (
  dispatch,
  getState: GetState
) => {
  try {
    const {
      orgs: {org},
    } = getState()

    await api.postDocumentsTemplate({
      data: {
        ...template,
        orgID: org.id,
      },
    })
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
    const {
      orgs: {org},
    } = getState()

    await api.postDocumentsTemplate({
      data: {
        ...resource,
        orgID: org.id,
      },
    })
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
    const resp = await api.putDocumentsTemplate({templateID: id, data: props})
    if (resp.status !== 200) {
      throw new Error(
        "Couldn't update the document template based on the corresponding id"
      )
    }

    dispatch(setTemplateSummary(id, {...props, meta: resp.data.meta}))
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

    const resp = await api.getDocumentsTemplate({templateID: id})
    if (resp.status !== 200) {
      throw new Error(
        'An error occurred trying to get the template based on the ID'
      )
    }
    const template = templateToExport(resp.data)

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
    const {
      orgs: {org},
    } = getState()

    const resp = await api.getDocumentsTemplate({templateID})

    if (resp.status !== 200) {
      throw new Error('An error occurred copying over the template')
    }

    const clone = resp.data
    const labels = clone.labels || []
    const labelIDs = labels.map(label => label.id) as Label[]
    clone.meta.name = `${clone.meta.name} (clone)`
    clone.labels = labelIDs

    const create = {
      meta: clone.meta,
      orgID: org.id,
      content: clone.content,
      labels: labelIDs,
    } as DocumentCreate

    const newTemplate = await api.postDocumentsTemplate({
      data: create,
    })

    if (newTemplate.status !== 201) {
      throw new Error('An error occurred cloning the template')
    }
    const items = {
      ...newTemplate.data,
      labels: newTemplate.data.labels || [],
    }
    dispatch(addTemplateSummary(items))
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
        return dispatch(createTaskFromTemplate(template as TaskTemplate))
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
  const resp = await api.getDocumentsTemplate({templateID})
  if (resp.status !== 200) {
    throw new Error(
      'An error occurred trying to get the template based on the ID'
    )
  }
  dispatch(createFromTemplate(resp.data))
}

export const addTemplateLabelAsync = (
  templateID: string,
  label: Label
) => async (dispatch): Promise<void> => {
  try {
    await api.postDocumentsTemplatesLabel({
      templateID,
      data: {labelID: label.id},
    })
    const resp = await api.getDocumentsTemplate({templateID})

    if (resp.status !== 200) {
      throw new Error('An error occurred adding the label to template')
    }

    dispatch(setTemplateSummary(templateID, templateToSummary(resp.data)))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addTemplateLabelFailed()))
  }
}

export const removeTemplateLabelAsync = (
  templateID: string,
  label: Label
) => async (dispatch): Promise<void> => {
  try {
    await api.deleteDocumentsTemplatesLabel({templateID, labelID: label.id})
    const resp = await api.getDocumentsTemplate({templateID})

    if (resp.status !== 200) {
      throw new Error('An error occurred deleting the template label')
    }

    dispatch(setTemplateSummary(templateID, templateToSummary(resp.data)))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removeTemplateLabelFailed()))
  }
}

const templateToSummary = (template: Template | Document): TemplateSummary => ({
  id: template.id,
  meta: template.meta,
  labels: template.labels,
})

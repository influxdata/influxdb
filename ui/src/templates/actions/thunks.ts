// Libraries
import {normalize} from 'normalizr'

// APIs
import {client} from 'src/utils/api'
import {createDashboardFromTemplate} from 'src/dashboards/actions/thunks'
import {createVariableFromTemplate} from 'src/variables/actions/thunks'
import {createTaskFromTemplate} from 'src/tasks/actions/thunks'

// Schemas
import * as schemas from 'src/schemas'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  addTemplateSummary,
  populateTemplateSummaries,
  setExportTemplate,
  setTemplatesStatus,
  removeTemplateSummary,
  setTemplateSummary,
  Action as TemplateAction,
} from 'src/templates/actions/creators'

// Constants
import * as copy from 'src/shared/copy/notifications'
import {staticTemplates} from 'src/templates/constants/defaultTemplates'

// Types
import {Dispatch} from 'react'
import {DocumentCreate, ITaskTemplate, TemplateType} from '@influxdata/influx'
import {
  RemoteDataState,
  GetState,
  DashboardTemplate,
  VariableTemplate,
  Template,
  TemplateSummary,
  TemplateSummaryEntities,
  Label,
} from 'src/types'

// Utils
import {templateToExport} from 'src/shared/utils/resourceToTemplate'
import {getOrg} from 'src/organizations/selectors'

type Action = TemplateAction | NotifyAction

export const getTemplateByID = async (id: string): Promise<Template> => {
  const template = (await client.templates.get(id)) as Template
  return template
}

export const getTemplates = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  const org = getOrg(getState())
  dispatch(setTemplatesStatus(RemoteDataState.Loading))
  const items = await client.templates.getAll(org.id)
  const templateSummaries = normalize<
    TemplateSummary,
    TemplateSummaryEntities,
    string[]
  >(items, schemas.arrayOfTemplates)
  dispatch(populateTemplateSummaries(templateSummaries))
}

export const createTemplate = (template: DocumentCreate) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())
    const item = await client.templates.create({...template, orgID: org.id})
    const templateSummary = normalize<
      TemplateSummary,
      TemplateSummaryEntities,
      string
    >(item, schemas.template)
    dispatch(addTemplateSummary(templateSummary))
    dispatch(notify(copy.importTemplateSucceeded()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.importTemplateFailed(e)))
  }
}

export const createTemplateFromResource = (
  resource: DocumentCreate,
  resourceName: string
) => async (dispatch: Dispatch<Action>, getState: GetState) => {
  try {
    const org = getOrg(getState())
    await client.templates.create({...resource, orgID: org.id})
    dispatch(notify(copy.resourceSavedAsTemplate(resourceName)))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.saveResourceAsTemplateFailed(resourceName, e)))
  }
}

export const updateTemplate = (id: string, props: TemplateSummary) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  setTemplateSummary(id, RemoteDataState.Loading)
  try {
    const item = await client.templates.update(id, props)
    const templateSummary = normalize<
      TemplateSummary,
      TemplateSummaryEntities,
      string
    >(item, schemas.template)

    dispatch(setTemplateSummary(id, RemoteDataState.Done, templateSummary))
    dispatch(notify(copy.updateTemplateSucceeded()))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.updateTemplateFailed(e)))
  }
}

export const convertToTemplate = (id: string) => async (
  dispatch: Dispatch<Action>
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

export const clearExportTemplate = () => (dispatch: Dispatch<Action>) => {
  dispatch(setExportTemplate(RemoteDataState.NotStarted, null))
}

export const deleteTemplate = (templateID: string) => async (
  dispatch: Dispatch<Action>
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
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  try {
    const org = getOrg(getState())
    const createdTemplate = await client.templates.clone(templateID, org.id)
    const templateSummary = normalize<
      TemplateSummary,
      TemplateSummaryEntities,
      string
    >(createdTemplate, schemas.template)

    dispatch(addTemplateSummary(templateSummary))
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
) => async (dispatch: Dispatch<Action>): Promise<void> => {
  try {
    await client.templates.addLabels(templateID, labels.map(l => l.id))
    const item = await client.templates.get(templateID)
    const templateSummary = normalize<
      TemplateSummary,
      TemplateSummaryEntities,
      string
    >(item, schemas.template)

    dispatch(
      setTemplateSummary(templateID, RemoteDataState.Done, templateSummary)
    )
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addTemplateLabelFailed()))
  }
}

export const removeTemplateLabelsAsync = (
  templateID: string,
  labels: Label[]
) => async (dispatch: Dispatch<Action>): Promise<void> => {
  try {
    await client.templates.removeLabels(templateID, labels.map(l => l.id))
    const item = await client.templates.get(templateID)
    const templateSummary = normalize<
      TemplateSummary,
      TemplateSummaryEntities,
      string
    >(item, schemas.template)

    dispatch(
      setTemplateSummary(templateID, RemoteDataState.Done, templateSummary)
    )
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removeTemplateLabelFailed()))
  }
}

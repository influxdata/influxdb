// Libraries
import {normalize} from 'normalizr'

// APIs
import {client} from 'src/utils/api'
import {fetchStacks, fetchReadMe} from 'src/templates/api'

// Schemas
import {arrayOfTemplates} from 'src/schemas/templates'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  setStacks,
  populateTemplateSummaries,
  setExportTemplate,
  setTemplatesStatus,
  setTemplateReadMe,
  Action as TemplateAction,
} from 'src/templates/actions/creators'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {Dispatch} from 'react'
import {
  RemoteDataState,
  GetState,
  TemplateSummary,
  TemplateSummaryEntities,
  Template,
  ResourceType,
} from 'src/types'

// Utils
import {templateToExport} from 'src/shared/utils/resourceToTemplate'
import {getOrg} from 'src/organizations/selectors'
import {getStatus} from 'src/resources/selectors'
import {readMeFormatter} from 'src/templates/utils'

type Action = TemplateAction | NotifyAction

export const getTemplateByID = async (id: string): Promise<Template> => {
  const template: Template = (await client.templates.get(id)) as any
  return template
}

export const getTemplates = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
): Promise<void> => {
  const state = getState()
  if (getStatus(state, ResourceType.Templates) === RemoteDataState.NotStarted) {
    dispatch(setTemplatesStatus(RemoteDataState.Loading))
  }

  const org = getOrg(state)

  const items = await client.templates.getAll(org.id)
  const templateSummaries = normalize<
    TemplateSummary,
    TemplateSummaryEntities,
    string[]
  >(items, arrayOfTemplates)
  dispatch(populateTemplateSummaries(templateSummaries))
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

// community template thunks

export const fetchAndSetStacks = (orgID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const stacks = await fetchStacks(orgID)
    dispatch(setStacks(stacks))
  } catch (error) {
    console.error(error)
  }
}

export const fetchAndSetReadme = (name: string, directory: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const response = await fetchReadMe(directory)
    const readme = readMeFormatter(response)
    dispatch(setTemplateReadMe(name, readme))
  } catch (error) {
    dispatch(
      setTemplateReadMe(
        name,
        "###### We can't find the readme associated with this template"
      )
    )
  }
}

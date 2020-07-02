// Types
import {
  CommunityTemplate,
  RemoteDataState,
  TemplateSummaryEntities,
} from 'src/types'
import {DocumentCreate} from '@influxdata/influx'
import {NormalizedSchema} from 'normalizr'

export const ADD_TEMPLATE_SUMMARY = 'ADD_TEMPLATE_SUMMARY'
export const GET_TEMPLATE_SUMMARIES_FOR_ORG = 'GET_TEMPLATE_SUMMARIES_FOR_ORG'
export const POPULATE_TEMPLATE_SUMMARIES = 'POPULATE_TEMPLATE_SUMMARIES'
export const REMOVE_TEMPLATE_SUMMARY = 'REMOVE_TEMPLATE_SUMMARY'
export const SET_COMMUNITY_TEMPLATE_TO_INSTALL =
  'SET_COMMUNITY_TEMPLATE_TO_INSTALL'
export const SET_EXPORT_TEMPLATE = 'SET_EXPORT_TEMPLATE'
export const SET_TEMPLATE_SUMMARY = 'SET_TEMPLATE_SUMMARY'
export const SET_TEMPLATES_STATUS = 'SET_TEMPLATES_STATUS'
export const TOGGLE_TEMPLATE_RESOURCE_INSTALL =
  'TOGGLE_TEMPLATE_RESOURCE_INSTALL'

export type Action =
  | ReturnType<typeof addTemplateSummary>
  | ReturnType<typeof populateTemplateSummaries>
  | ReturnType<typeof removeTemplateSummary>
  | ReturnType<typeof setExportTemplate>
  | ReturnType<typeof setTemplatesStatus>
  | ReturnType<typeof setTemplateSummary>
  | ReturnType<typeof setCommunityTemplateToInstall>
  | ReturnType<typeof toggleTemplateResourceInstall>

type TemplateSummarySchema<R extends string | string[]> = NormalizedSchema<
  TemplateSummaryEntities,
  R
>

// Action Creators
export const addTemplateSummary = (schema: TemplateSummarySchema<string>) =>
  ({
    type: ADD_TEMPLATE_SUMMARY,
    schema,
  } as const)

export const populateTemplateSummaries = (
  schema: TemplateSummarySchema<string[]>
) =>
  ({
    type: POPULATE_TEMPLATE_SUMMARIES,
    status: RemoteDataState.Done,
    schema,
  } as const)

export const setExportTemplate = (
  status: RemoteDataState,
  item?: DocumentCreate
) =>
  ({
    type: SET_EXPORT_TEMPLATE,
    status,
    item,
  } as const)

export const setTemplatesStatus = (status: RemoteDataState) =>
  ({
    type: SET_TEMPLATES_STATUS,
    status,
  } as const)

export const removeTemplateSummary = (id: string) =>
  ({
    type: REMOVE_TEMPLATE_SUMMARY,
    id,
  } as const)

export const setTemplateSummary = (
  id: string,
  status: RemoteDataState,
  schema?: TemplateSummarySchema<string>
) =>
  ({
    type: SET_TEMPLATE_SUMMARY,
    id,
    status,
    schema,
  } as const)

export const setCommunityTemplateToInstall = (template: CommunityTemplate) =>
  ({
    type: SET_COMMUNITY_TEMPLATE_TO_INSTALL,
    template,
  } as const)

export const toggleTemplateResourceInstall = (
  resourceType: string,
  templateMetaName: string,
  shouldInstall: boolean
) =>
  ({
    type: TOGGLE_TEMPLATE_RESOURCE_INSTALL,
    resourceType,
    templateMetaName,
    shouldInstall,
  } as const)

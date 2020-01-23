// Types
import {RemoteDataState, TemplateSummaryEntities} from 'src/types'
import {DocumentCreate} from '@influxdata/influx'
import {NormalizedSchema} from 'normalizr'

export const ADD_TEMPLATE_SUMMARY = 'ADD_TEMPLATE_SUMMARY'
export const GET_TEMPLATE_SUMMARIES_FOR_ORG = 'GET_TEMPLATE_SUMMARIES_FOR_ORG'
export const POPULATE_TEMPLATE_SUMMARIES = 'POPULATE_TEMPLATE_SUMMARIES'
export const REMOVE_TEMPLATE_SUMMARY = 'REMOVE_TEMPLATE_SUMMARY'
export const SET_EXPORT_TEMPLATE = 'SET_EXPORT_TEMPLATE'
export const SET_TEMPLATE_SUMMARY = 'SET_TEMPLATE_SUMMARY'
export const SET_TEMPLATES_STATUS = 'SET_TEMPLATES_STATUS'

export type Action =
  | ReturnType<typeof addTemplateSummary>
  | ReturnType<typeof populateTemplateSummaries>
  | ReturnType<typeof setExportTemplate>
  | ReturnType<typeof setTemplatesStatus>
  | ReturnType<typeof removeTemplateSummary>
  | ReturnType<typeof setTemplateSummary>

// R
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

export const removeTemplateSummary = (templateID: string) =>
  ({
    type: REMOVE_TEMPLATE_SUMMARY,
    templateID,
  } as const)

export const setTemplateSummary = (
  templateID: string,
  status: RemoteDataState,
  schema: TemplateSummarySchema<string>
) =>
  ({
    type: SET_TEMPLATE_SUMMARY,
    templateID,
    status,
    schema,
  } as const)

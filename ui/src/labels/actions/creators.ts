// Types
import {RemoteDataState, LabelEntities} from 'src/types'
import {Action as NotifyAction} from 'src/shared/actions/notifications'
import {NormalizedSchema} from 'normalizr'

export type Action =
  | ReturnType<typeof removeLabel>
  | ReturnType<typeof setLabel>
  | ReturnType<typeof setLabels>
  | NotifyAction

export const SET_LABELS = 'SET_LABELS'
export const SET_LABEL = 'SET_LABEL'
export const REMOVE_LABEL = 'REMOVE_LABEL'
export const SET_LABEL_ON_RESOURCE = 'SET_LABEL_ON_RESOURCE'

type LabelsSchema<R extends string | string[]> = NormalizedSchema<
  LabelEntities,
  R
>

export const setLabels = (
  status: RemoteDataState,
  schema?: LabelsSchema<string[]>
) =>
  ({
    type: SET_LABELS,
    status,
    schema,
  } as const)

export const setLabel = (
  id: string,
  status: RemoteDataState,
  schema?: LabelsSchema<string>
) =>
  ({
    type: SET_LABEL,
    id,
    status,
    schema,
  } as const)

export const removeLabel = (id: string) =>
  ({
    type: REMOVE_LABEL,
    id,
  } as const)

export const setLabelOnResource = (
  resourceID: string,
  schema: LabelsSchema<string>
) =>
  ({
    type: SET_LABEL_ON_RESOURCE,
    resourceID,
    schema,
  } as const)

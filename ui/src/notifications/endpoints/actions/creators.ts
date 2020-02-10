// Libraries
import {NormalizedSchema} from 'normalizr'

// Types
import {RemoteDataState, EndpointEntities} from 'src/types'
import {setLabelOnResource} from 'src/labels/actions/creators'

export const SET_ENDPOINT = 'SET_ENDPOINT'
export const REMOVE_ENDPOINT = 'REMOVE_ENDPOINT'
export const SET_ENDPOINTS = 'SET_ENDPOINTS'
export const ADD_LABEL_TO_ENDPOINT = 'ADD_LABEL_TO_ENDPOINT'
export const REMOVE_LABEL_FROM_ENDPOINT = 'REMOVE_LABEL_FROM_ENDPOINT'

export type Action =
  | ReturnType<typeof setEndpoint>
  | ReturnType<typeof removeEndpoint>
  | ReturnType<typeof setEndpoints>
  | ReturnType<typeof setLabelOnResource>
  | ReturnType<typeof removeLabelFromEndpoint>

type EndpointsSchema<R extends string | string[]> = NormalizedSchema<
  EndpointEntities,
  R
>

export const removeEndpoint = (id: string) =>
  ({type: REMOVE_ENDPOINT, id} as const)

export const setEndpoint = (
  id: string,
  status: RemoteDataState,
  schema?: EndpointsSchema<string>
) =>
  ({
    type: SET_ENDPOINT,
    id,
    status,
    schema,
  } as const)

export const setEndpoints = (
  status: RemoteDataState,
  schema?: EndpointsSchema<string[]>
) =>
  ({
    type: SET_ENDPOINTS,
    status,
    schema,
  } as const)

export const removeLabelFromEndpoint = (endpointID: string, labelID: string) =>
  ({
    type: REMOVE_LABEL_FROM_ENDPOINT,
    endpointID,
    labelID,
  } as const)

// Types
import {RemoteDataState, Schema} from 'src/types'

export const SET_SCHEMA = 'SET_SCHEMA'
export const RESET_SCHEMA = 'RESET_SCHEMA'
export const REMOVE_SCHEMA = 'REMOVE_SCHEMA'

export type Action =
  | ReturnType<typeof setSchema>
  | ReturnType<typeof removeSchema>
  | ReturnType<typeof resetSchema>

export const setSchema = (
  status: RemoteDataState,
  bucketName?: string,
  schema?: Schema | object
) =>
  ({
    type: SET_SCHEMA,
    bucketName,
    schema,
    status,
  } as const)

export const removeSchema = (bucketName: string) =>
  ({
    type: REMOVE_SCHEMA,
    bucketName,
  } as const)

export const resetSchema = () =>
  ({
    type: RESET_SCHEMA,
  } as const)

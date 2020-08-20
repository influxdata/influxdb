// Types
import {RemoteDataState} from 'src/types'

export const SET_SCHEMA = 'SET_SCHEMA'
export const REMOVE_SCHEMA = 'REMOVE_SCHEMA'

export type Action =
  | ReturnType<typeof setSchema>
  | ReturnType<typeof removeSchema>

export const setSchema = (
  status: RemoteDataState,
  bucketName?: string,
  schema?: any[]
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

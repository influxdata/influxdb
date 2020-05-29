// Types
import {RemoteDataState, BucketEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'

export const SET_BUCKETS = 'SET_BUCKETS'
export const ADD_BUCKET = 'ADD_BUCKET'
export const EDIT_BUCKET = 'EDIT_BUCKET'
export const REMOVE_BUCKET = 'REMOVE_BUCKET'

export type Action =
  | ReturnType<typeof setBuckets>
  | ReturnType<typeof addBucket>
  | ReturnType<typeof editBucket>
  | ReturnType<typeof removeBucket>

export const setBuckets = (
  status: RemoteDataState,
  schema?: NormalizedSchema<BucketEntities, string[]>
) =>
  ({
    type: SET_BUCKETS,
    status,
    schema,
  } as const)

export const addBucket = (schema: NormalizedSchema<BucketEntities, string>) =>
  ({
    type: ADD_BUCKET,
    schema,
  } as const)

export const editBucket = (schema: NormalizedSchema<BucketEntities, string>) =>
  ({
    type: EDIT_BUCKET,
    schema,
  } as const)

export const removeBucket = (id: string) =>
  ({
    type: REMOVE_BUCKET,
    id,
  } as const)

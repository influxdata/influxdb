// Types
import {AppState} from 'src/types'

export const getSchemaByBucket = (state: AppState, bucketName: string) =>
  state.notebook.schema[bucketName]

export const getStatusByBucket = (state: AppState, bucketName: string) =>
  state.notebook.schema[bucketName].status

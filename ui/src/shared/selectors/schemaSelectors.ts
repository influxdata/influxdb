// Types
import {AppState} from 'src/types'
import {BucketSchema} from 'src/shared/reducers/schema'

export const getSchemaByBucketName = (
  state: AppState,
  bucketName: string
): BucketSchema | null => {
  return state.notebook.schema[bucketName] || null
}

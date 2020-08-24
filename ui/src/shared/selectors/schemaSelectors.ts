// Types
import {AppState, Schema} from 'src/types'

export const getSchemaByBucketName = (
  state: AppState,
  bucketName: string
): Schema | null => {
  return state.notebook.schema[bucketName] || null
}

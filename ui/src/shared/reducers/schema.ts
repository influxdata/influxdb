import {
  RESET_SCHEMA,
  SET_SCHEMA,
  Action,
  REMOVE_SCHEMA,
} from 'src/shared/actions/schemaCreator'
// Types
import {RemoteDataState, Schema} from 'src/types'

export const TEN_MINUTES = 10 * 60 * 1000 // 10 muinutes in ms
export interface BucketSchema {
  schema: Schema
  exp: number
  status: RemoteDataState
}

interface ReduxSchema {
  bucketName: BucketSchema
}

export type SchemaState = {
  status: RemoteDataState
  schema: ReduxSchema | object
}

const initialState = (): SchemaState => ({
  status: RemoteDataState.NotStarted,
  schema: {},
})

export const schemaReducer = (
  state: SchemaState = initialState(),
  action: Action
): SchemaState => {
  switch (action.type) {
    case SET_SCHEMA: {
      const {bucketName, schema, status} = action
      return {
        ...state,
        schema: {
          [bucketName]: {
            exp: new Date().getTime() + TEN_MINUTES,
            schema,
            status,
          },
        },
      }
    }

    case REMOVE_SCHEMA: {
      const {bucketName} = action
      const {schema} = state
      if (bucketName in schema) {
        delete schema[bucketName]
      }
      return state
    }
    case RESET_SCHEMA: {
      const {schema} = state
      const now = Date.now()
      const newSchema = {...schema}
      for (const bucket in schema) {
        if (now >= schema[bucket].exp) {
          delete newSchema[bucket]
        }
      }
      return {
        ...state,
        schema: newSchema,
      }
    }

    default: {
      return state
    }
  }
}

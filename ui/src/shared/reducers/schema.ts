import {
  SET_SCHEMA,
  Action,
  REMOVE_SCHEMA,
} from 'src/shared/actions/schemaCreator'
// Types
import {RemoteDataState, Schema} from 'src/types'

interface ReduxSchema {
  bucketName: {
    schema: Schema
    status: RemoteDataState
  }
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

    default: {
      return state
    }
  }
}

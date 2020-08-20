// Types
import {RemoteDataState} from 'src/types'
import {
  SET_SCHEMA,
  Action,
  REMOVE_SCHEMA,
} from 'src/shared/actions/schemaCreator'

export type SchemaState = {
  status: RemoteDataState
  schema: {
    [bucketName: string]: {
      schema: any[]
      status: RemoteDataState
    }
  }
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

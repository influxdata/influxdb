// Types
import {RemoteDataState} from 'src/types'
import {SET_SCHEMA, Action, REMOVE_SCHEMA} from 'src/notebooks/actions/creators'

type SchemaState = {
  status: RemoteDataState
  schema: {
    [key: string]: any[]
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
      const {bucketName, schema} = action
      return {
        ...state,
        schema: {
          [bucketName]: schema,
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
  }
}

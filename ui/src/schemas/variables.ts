// Libraries
import {schema} from 'normalizr'

// Types
import {RemoteDataState, ResourceType, Variable} from 'src/types'
import {labelSchema} from './labels'

// Defines the schema for the "variables" resource
export const variableSchema = new schema.Entity(
  ResourceType.Variables,
  {
    labels: [labelSchema],
  },
  {
    processStrategy: (v: Variable): Variable => {
      return {
        ...v,
        status: RemoteDataState.Done,
      }
    },
  }
)
export const arrayOfVariables = [variableSchema]

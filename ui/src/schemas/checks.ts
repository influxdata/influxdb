// Libraries
import {schema} from 'normalizr'

// Types
import {RemoteDataState, ResourceType, GenCheck, Check} from 'src/types'

// Schemas
import {arrayOfLabels} from './labels'

/* Checks */

// Defines the schema for the "checks" resource
export const checkSchema = new schema.Entity(
  ResourceType.Checks,
  {
    labels: arrayOfLabels,
  },
  {
    processStrategy: (check: GenCheck): Omit<Check, 'labels'> => {
      if (!check.query.editMode){
        return {
          ...check,
          query: generateBuilderConfig(check.query),
          status: RemoteDataState.Done,
          activeStatus: check.status,
        }
      }
      
      return {
        ...check,
        status: RemoteDataState.Done,
        activeStatus: check.status,
      }
    },
  }
)

export const arrayOfChecks = [checkSchema]

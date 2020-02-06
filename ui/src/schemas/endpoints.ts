// Libraries
import {schema} from 'normalizr'

// Types
import {
  RemoteDataState,
  ResourceType,
  NotificationEndpoint,
  GenEndpoint,
} from 'src/types'

// Schemas
import {arrayOfLabels} from './labels'

/* Endpoints */
export const endpointSchema = new schema.Entity(
  ResourceType.NotificationEndpoints,
  {
    labels: arrayOfLabels,
  },
  {
    processStrategy: point => addEndpointDefaults(point),
  }
)

export const arrayOfEndpoints = [endpointSchema]

const addEndpointDefaults = (
  point: GenEndpoint
): Omit<NotificationEndpoint, 'labels'> => {
  return {
    ...point,
    status: RemoteDataState.Done,
    activeStatus: point.status,
  }
}

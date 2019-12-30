// Libraries
import {schema} from 'normalizr'

// Types
import {Member, RemoteDataState} from 'src/types'

// ResourceState defines the types for normalized resources
export interface ResourcesState {
  resources: {
    members: {
      byID: {
        [uuid: string]: Member
      }
      allIDs: string[]
      status: RemoteDataState
    }
  }
}

// Entities defines the result of normalizr's normalization
export interface Entities {
  members: {
    [uuid: string]: Member
  }
}

// Defines the schema for the "member" resource
export const members = new schema.Entity('members')

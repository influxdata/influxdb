// Libraries
import {schema} from 'normalizr'

// Types
import {Member, ResourceType} from 'src/types'

// Entities defines the result of normalizr's normalization
export interface MemberEntities {
  members: {
    [uuid: string]: Member
  }
}

// Defines the schema for the "member" resource
export const members = new schema.Entity(ResourceType.Members)

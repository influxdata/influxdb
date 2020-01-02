// Libraries
import {schema} from 'normalizr'

// Types
import {Member, ResourceType, Organization} from 'src/types'

// Entities defines the result of normalizr's normalization
export interface MemberEntities {
  members: {
    [uuid: string]: Member
  }
}

export interface OrgEntities {
  orgs: {
    [uuid: string]: Organization
  }
}

// Defines the schema for the "member" resource
export const members = new schema.Entity(ResourceType.Members)

// Defines the schema for the "member" resource
export const orgs = new schema.Entity(ResourceType.Orgs)

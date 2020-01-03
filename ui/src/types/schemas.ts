// Types
import {Member, Bucket, Organization} from 'src/types'

// TODO: make these Entities generic

// MemberEntities defines the result of normalizr's normalization
// of the "member" resource
export interface MemberEntities {
  members: {
    [uuid: string]: Member
  }
}

// OrgEntities defines the result of normalizr's normalization
// of the "organizations" resource
export interface OrgEntities {
  orgs: {
    [uuid: string]: Organization
  }
}

// BucketEntities defines the result of normalizr's normalization
// of the "organizations" resource
export interface BucketEntities {
  buckets: {
    [uuid: string]: Bucket
  }
}

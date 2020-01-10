// Types
import {
  Task,
  Telegraf,
  Member,
  Bucket,
  Scraper,
  Organization,
  Authorization,
} from 'src/types'

// TODO: make these Entities generic

// AuthEntities defines the result of normalizr's normalization
// of the "authorizations" resource
export interface AuthEntities {
  buckets: {
    [uuid: string]: Authorization
  }
}

// BucketEntities defines the result of normalizr's normalization
// of the "buckets" resource
export interface BucketEntities {
  buckets: {
    [uuid: string]: Bucket
  }
}

// MemberEntities defines the result of normalizr's normalization
// of the "members" resource
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

// TelegrafEntities defines the result of normalizr's normalization
// of the "telegrafs" resource
export interface TelegrafEntities {
  telegrafs: {
    [uuid: string]: Telegraf
  }
}

// ScraperEntities defines the result of normalizr's normalization
// of the "scrapers" resource
export interface ScraperEntities {
  scrapers: {
    [uuid: string]: Scraper
  }
}

// TaskEntities defines the result of normalizr's normalization
// of the "tasks" resource
export interface TaskEntities {
  tasks: {
    [uuid: string]: Task
  }
}

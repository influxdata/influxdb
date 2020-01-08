// Types
import {
  Telegraf,
  Member,
  Bucket,
  Scraper,
  Organization,
  Authorization,
} from 'src/types'

// TODO: make these Entities generic

// AuthEntities defines the result of normalizr's normalization
// of the "authorization" resource
export interface AuthEntities {
  buckets: {
    [uuid: string]: Authorization
  }
}

// BucketEntities defines the result of normalizr's normalization
// of the "bucket" resource
export interface BucketEntities {
  buckets: {
    [uuid: string]: Bucket
  }
}

// MemberEntities defines the result of normalizr's normalization
// of the "member" resource
export interface MemberEntities {
  members: {
    [uuid: string]: Member
  }
}

// OrgEntities defines the result of normalizr's normalization
// of the "organization" resource
export interface OrgEntities {
  orgs: {
    [uuid: string]: Organization
  }
}

// TelegrafEntities defines the result of normalizr's normalization
// of the "telegraf" resource
export interface TelegrafEntities {
  telegrafs: {
    [uuid: string]: Telegraf
  }
}

// ScraperEntities defines the result of normalizr's normalization
// of the "scraper" resource
export interface ScraperEntities {
  scrapers: {
    [uuid: string]: Scraper
  }
}

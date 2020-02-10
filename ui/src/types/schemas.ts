// Types
import {
  Authorization,
  Bucket,
  Cell,
  Check,
  Dashboard,
  Label,
  Member,
  Organization,
  Scraper,
  Task,
  Telegraf,
  TemplateSummary,
  Variable,
  View,
  NotificationRule,
  NotificationEndpoint,
} from 'src/types'

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

// CellEntities defines the result of normalizr's normalization
// of the "cells" resource
export interface CellEntities {
  cells: {
    [uuid: string]: Cell
  }
}

// CheckEntities defines the result of normalizr's normalization of
// the "checks" resource
export interface CheckEntities {
  checks: {
    [uuid: string]: Check
  }
}

// DashboardEntities defines the result of normalizr's normalization
// of the "dashboards" resource
export interface DashboardEntities {
  dashboards: {
    [uuid: string]: Dashboard
  }
  cells: {
    [uuid: string]: Cell
  }
}

// EndpointEntities defines the result of normalizr's normalization of
// the "notification_endpoint" resource
export interface EndpointEntities {
  endpoints: {
    [uuid: string]: NotificationEndpoint
  }
}

// LabelEntities defines the result of normalizr's normalization of
// the "labels" resource
export interface LabelEntities {
  labels: {
    [uuid: string]: Label
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

// RuleEntities defines the result of normalizr's normalization
// of the "notification rules" resource
export interface RuleEntities {
  rules: {
    [uuid: string]: NotificationRule
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

// TelegrafEntities defines the result of normalizr's normalization
// of the "telegrafs" resource
export interface TelegrafEntities {
  telegrafs: {
    [uuid: string]: Telegraf
  }
}

// TemplateSummaryEntities defines the result of normalizr's normalization
// of the "templates resource"
export interface TemplateSummaryEntities {
  templates: {
    [uuid: string]: TemplateSummary
  }
}

// VariableEntities defines the result of normalizr's normalization
// of the "variables" resource
export interface VariableEntities {
  variables: {
    [uuid: string]: Variable
  }
}

export interface ViewEntities {
  views: {
    [uuid: string]: View
  }
}

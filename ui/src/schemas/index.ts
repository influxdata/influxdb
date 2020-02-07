// Libraries
import {schema} from 'normalizr'
import {omit} from 'lodash'

// Types
import {
  Cell,
  Dashboard,
  Label,
  RemoteDataState,
  ResourceType,
  View,
} from 'src/types'
import {CellsWithViewProperties} from 'src/client'

// Utils
import {addLabelDefaults} from 'src/labels/utils'
import {defaultView} from 'src/views/helpers'
import {arrayOfLabels} from './labels'

/* Buckets */

// Defines the schema for the "buckets" resource
export const bucket = new schema.Entity(ResourceType.Buckets)
export const arrayOfBuckets = [bucket]

/* Views */

// Defines the schema for the "views" resource
export const viewsFromCells = (
  cells: CellsWithViewProperties,
  dashboardID: string
): View[] => {
  return cells.map(cell => {
    const {properties, id, name} = cell

    return {
      id,
      ...defaultView(name),
      cellID: id,
      properties,
      dashboardID,
    }
  })
}

export const view = new schema.Entity(ResourceType.Views)
export const arrayOfViews = [view]

/* Cells */

// Defines the schema for the "cells" resource
export const cell = new schema.Entity(
  ResourceType.Cells,
  {},
  {
    processStrategy: (cell: Cell, parent: Dashboard) => {
      return {
        ...omit<Cell>(cell, 'properties'),
        dashboardID: cell.dashboardID ? cell.dashboardID : parent.id,
        status: RemoteDataState.Done,
      }
    },
  }
)
export const arrayOfCells = [cell]

/* Dashboards */

// Defines the schema for the "dashboards" resource
export const dashboard = new schema.Entity(
  ResourceType.Dashboards,
  {
    labels: arrayOfLabels,
    cells: arrayOfCells,
    views: arrayOfViews,
  },
  {
    processStrategy: (dashboard: Dashboard) => addDashboardDefaults(dashboard),
  }
)
export const arrayOfDashboards = [dashboard]

export const addDashboardDefaults = (dashboard: Dashboard): Dashboard => {
  return {
    ...dashboard,
    id: dashboard.id || '',
    name: dashboard.name || '',
    orgID: dashboard.orgID || '',
    meta: addDashboardMetaDefaults(dashboard.meta),
    status: RemoteDataState.Done,
  }
}

const addDashboardMetaDefaults = (meta: Dashboard['meta']) => {
  if (!meta) {
    return {}
  }

  if (!meta.updatedAt) {
    return {...meta, updatedAt: new Date().toDateString()}
  }

  return meta
}

/* Members */

// Defines the schema for the "members" resource
export const member = new schema.Entity(ResourceType.Members)
export const arrayOfMembers = [member]

/* Organizations */

// Defines the schema for the "organizations" resource
export const org = new schema.Entity(ResourceType.Orgs)
export const arrayOfOrgs = [org]

/* Scrapers */

// Defines the schema for the "scrapers" resource

export const scraper = new schema.Entity(ResourceType.Scrapers)
export const arrayOfScrapers = [scraper]

export const addLabels = <R extends {labels?: Label[]}>(
  resource: R
): Label[] => {
  return (resource.labels || []).map(addLabelDefaults)
}

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
  Task,
  Telegraf,
  Variable,
  View,
} from 'src/types'
import {CellsWithViewProperties} from 'src/client'

// Utils
import {addLabelDefaults} from 'src/labels/utils'
import {defaultView} from 'src/views/helpers'

/* Authorizations */

// Defines the schema for the "authorizations" resource
export const auth = new schema.Entity(ResourceType.Authorizations)
export const arrayOfAuths = [auth]

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
    labels: (dashboard.labels || []).map(addLabelDefaults),
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

/* Tasks */

// Defines the schema for the tasks resource
export const task = new schema.Entity(
  ResourceType.Tasks,
  {},
  {
    processStrategy: (task: Task): Task => ({
      ...task,
      labels: addLabels(task),
    }),
  }
)

export const arrayOfTasks = [task]

/* Telegrafs */

// Defines the schema for the "telegrafs" resource
export const telegraf = new schema.Entity(
  ResourceType.Telegrafs,
  {},
  {
    // add buckets to metadata if not present
    processStrategy: (t: Telegraf): Telegraf => {
      if (!t.metadata) {
        return {
          ...t,
          metadata: {
            buckets: [],
          },
        }
      }

      if (!t.metadata.buckets) {
        return {
          ...t,
          metadata: {
            ...t.metadata,
            buckets: [],
          },
        }
      }

      return t
    },
  }
)

export const arrayOfTelegrafs = [telegraf]

/* Templates */

// Defines the schema for the "templates" resource
export const template = new schema.Entity(ResourceType.Templates)
export const arrayOfTemplates = [template]

/* Scrapers */

// Defines the schema for the "scrapers" resource

export const scraper = new schema.Entity(ResourceType.Scrapers)
export const arrayOfScrapers = [scraper]

/* Variables */

// Defines the schema for the "variables" resource
export const variable = new schema.Entity(
  ResourceType.Variables,
  {},
  {
    processStrategy: (v: Variable): Variable => {
      return {
        ...v,
        labels: addLabels(v),
        status: addStatus(v),
      }
    },
  }
)
export const arrayOfVariables = [variable]

// Defaults
const addStatus = <R extends {status: RemoteDataState}>(resource: R) => {
  return resource.status ? resource.status : RemoteDataState.Done
}

export const addLabels = <R extends {labels?: Label[]}>(
  resource: R
): Label[] => {
  return (resource.labels || []).map(addLabelDefaults)
}

// Libraries
import {schema} from 'normalizr'
import {omit} from 'lodash'

// Types
import {Cell, Dashboard, RemoteDataState, ResourceType, View} from 'src/types'
import {CellsWithViewProperties} from 'src/client'

// Utils
import {defaultView} from 'src/views/helpers'
import {arrayOfLabels} from './labels'

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

export const viewSchema = new schema.Entity(ResourceType.Views)
export const arrayOfViews = [viewSchema]

/* Cells */

// Defines the schema for the "cells" resource
export const cellSchema = new schema.Entity(
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
export const arrayOfCells = [cellSchema]

/* Dashboards */

// Defines the schema for the "dashboards" resource
export const dashboardSchema = new schema.Entity(
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
export const arrayOfDashboards = [dashboardSchema]

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

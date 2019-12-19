// Libraries
import {schema} from 'normalizr'

// Types
import {Dashboard, Label, Cell, View, RemoteDataState} from 'src/types'

export interface NDashboard extends Omit<Dashboard, 'cells' | 'labels'> {
  cells: string[]
  labels: string[]
}

export interface NCell extends Omit<Cell, 'views'> {
  views: string[]
}

export interface NormalizedAppState {
  resources: {
    dashboards: {
      byID: {
        [uuid: string]: NDashboard
      }
      allIDs: string[]
      status: RemoteDataState
    }
    cells: {
      byID: {
        [uuid: string]: NCell
      }
    }
    labels: {
      byID: {
        [uuid: string]: Label
      }
    }
    views: {
      byID: {
        [uuid: string]: View
      }
    }
  }
}

export interface Entities {
  dashboards: {
    [uuid: string]: NDashboard
  }
  cells: {
    [uuid: string]: NCell
  }
  labels: {
    [uuid: string]: Label
  }
  views: {
    [uuid: string]: View
  }
}

const views = new schema.Entity('views')

// Define cell schema
const cells = new schema.Entity(
  'cells',
  {
    views: [views],
  },
  {
    // add dashboardID to cell
    processStrategy: (entity, parent) => ({...entity, dashboardID: parent.id}),
  }
)

// Define label schema
const labels = new schema.Entity('labels')

// Define dashboard schema
export const dashboards = new schema.Entity('dashboards', {
  cells: [cells],
  labels: [labels],
})

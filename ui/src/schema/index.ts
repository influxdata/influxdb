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
      allIds: string[]
    }
    labels: {
      byID: {
        [uuid: string]: Label
      }
      allIDs: string[]
    }
    views: {
      byID: {
        [uuid: string]: View
      }
      allIDs: string[]
    }
  }
}

interface Entities {
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

const resp = [
  {
    id: '04f1705f8c282000',
    orgID: '28bfc6e53d69f139',
    name: 'Name this Dashboard',
    description: '',
    meta: {
      createdAt: '2019-12-17T13:29:30.928805-08:00',
      updatedAt: '2019-12-17T13:29:36.887609-08:00',
    },
    cells: [
      {
        id: '04f170653ea82000',
        x: 0,
        y: 0,
        w: 4,
        h: 4,
        links: {
          self: '/api/v2/dashboards/04f1705f8c282000/cells/04f170653ea82000',
          view:
            '/api/v2/dashboards/04f1705f8c282000/cells/04f170653ea82000/view',
        },
      },
    ],
    labels: [
      {
        id: '04f188dec9282001',
        orgID: '28bfc6e53d69f139',
        name: 'asdfasdfsdf',
        properties: {
          color: '#326BBA',
          description: '',
        },
      },
    ],
    links: {
      self: '/api/v2/dashboards/04f1705f8c282000',
      members: '/api/v2/dashboards/04f1705f8c282000/members',
      owners: '/api/v2/dashboards/04f1705f8c282000/owners',
      cells: '/api/v2/dashboards/04f1705f8c282000/cells',
      logs: '/api/v2/dashboards/04f1705f8c282000/logs',
      labels: '/api/v2/dashboards/04f1705f8c282000/labels',
      org: '/api/v2/orgs/28bfc6e53d69f139',
    },
  },
  {
    id: '04f1705f8c282001',
    orgID: '28bfc6e53d69f139',
    name: 'Name this Dashboard',
    description: '',
    meta: {
      createdAt: '2019-12-17T13:29:30.928805-08:00',
      updatedAt: '2019-12-17T13:29:36.887609-08:00',
    },
    cells: [
      {
        id: '04f170653ea82000',
        x: 0,
        y: 0,
        w: 4,
        h: 4,
        links: {
          self: '/api/v2/dashboards/04f1705f8c282000/cells/04f170653ea82000',
          view:
            '/api/v2/dashboards/04f1705f8c282000/cells/04f170653ea82000/view',
        },
      },
    ],
    labels: [
      {
        id: '04f188dec9282002',
        orgID: '28bfc6e53d69f139',
        name: 'asdfasdfsdf',
        properties: {
          color: '#326BBA',
          description: '',
        },
      },
    ],
    links: {
      self: '/api/v2/dashboards/04f1705f8c282000',
      members: '/api/v2/dashboards/04f1705f8c282000/members',
      owners: '/api/v2/dashboards/04f1705f8c282000/owners',
      cells: '/api/v2/dashboards/04f1705f8c282000/cells',
      logs: '/api/v2/dashboards/04f1705f8c282000/logs',
      labels: '/api/v2/dashboards/04f1705f8c282000/labels',
      org: '/api/v2/orgs/28bfc6e53d69f139',
    },
  },
]

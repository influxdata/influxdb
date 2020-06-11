// Libraries
import {produce} from 'immer'

// Types
import {
  RemoteDataState,
  ResourceState,
  Dashboard,
  ResourceType,
} from 'src/types'

// Actions
import {
  Action,
  SET_DASHBOARD,
  SET_DASHBOARD_SORT,
  REMOVE_DASHBOARD,
  SET_DASHBOARDS,
  REMOVE_DASHBOARD_LABEL,
  EDIT_DASHBOARD,
} from 'src/dashboards/actions/creators'
import {
  SET_CELLS,
  REMOVE_CELL,
  SET_CELL,
  Action as CellAction,
} from 'src/cells/actions/creators'
import {SET_LABEL_ON_RESOURCE} from 'src/labels/actions/creators'

// Utils
import {
  setResource,
  setResourceAtID,
  removeResource,
  editResource,
  setRelation,
} from 'src/resources/reducers/helpers'

import {DEFAULT_DASHBOARD_SORT_OPTIONS} from 'src/dashboards/constants'

type DashboardsState = ResourceState['dashboards']

const initialState = () => ({
  byID: {},
  allIDs: [],
  status: RemoteDataState.NotStarted,
  sortOptions: DEFAULT_DASHBOARD_SORT_OPTIONS,
})

export const dashboardsReducer = (
  state: DashboardsState = initialState(),
  action: Action | CellAction
): DashboardsState => {
  return produce(state, draftState => {
    switch (action.type) {
      case SET_DASHBOARDS: {
        setResource<Dashboard>(draftState, action, ResourceType.Dashboards)

        return
      }

      case REMOVE_DASHBOARD: {
        removeResource<Dashboard>(draftState, action)

        return
      }

      case SET_DASHBOARD: {
        setResourceAtID<Dashboard>(draftState, action, ResourceType.Dashboards)

        return
      }

      case SET_DASHBOARD_SORT: {
        const {sortOptions} = action

        draftState['sortOptions'] = sortOptions

        return
      }

      case EDIT_DASHBOARD: {
        editResource<Dashboard>(draftState, action, ResourceType.Dashboards)

        return
      }

      case REMOVE_CELL: {
        const {dashboardID, id} = action

        const {cells} = draftState.byID[dashboardID]

        draftState.byID[dashboardID].cells = cells.filter(cID => cID !== id)

        return
      }

      case SET_CELL: {
        const {schema} = action

        const cellID = schema.result
        const cell = schema.entities.cells[cellID]
        const {cells} = draftState.byID[cell.dashboardID]

        if (cells.includes(cellID)) {
          return
        }

        draftState.byID[cell.dashboardID].cells.push(cellID)

        return
      }

      case SET_CELLS: {
        const {dashboardID, schema} = action

        const cellIDs = schema && schema.result

        if (!cellIDs) {
          return
        }

        draftState.byID[dashboardID].cells = cellIDs

        return
      }

      case SET_LABEL_ON_RESOURCE: {
        const {resourceID, schema} = action
        const labelID = schema.result

        setRelation<Dashboard>(
          draftState,
          ResourceType.Labels,
          labelID,
          resourceID
        )

        return
      }

      case REMOVE_DASHBOARD_LABEL: {
        const {dashboardID, labelID} = action

        const {labels} = draftState.byID[dashboardID]

        draftState.byID[dashboardID].labels = labels.filter(
          label => label !== labelID
        )

        return
      }
    }
  })
}

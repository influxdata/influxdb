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
  REMOVE_DASHBOARD,
  SET_DASHBOARDS,
  REMOVE_CELL,
  REMOVE_DASHBOARD_LABEL,
  ADD_DASHBOARD_LABEL,
  EDIT_DASHBOARD,
} from 'src/dashboards/actions/creators'

// Utils
import {
  setResource,
  removeResource,
  editResource,
} from 'src/resources/reducers/helpers'

type DashboardsState = ResourceState['dashboards']

const initialState = () => ({
  byID: {},
  allIDs: [],
  status: RemoteDataState.NotStarted,
})

export const dashboardsReducer = (
  state: DashboardsState = initialState(),
  action: Action
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
        const {schema} = action
        const {entities, result} = schema

        draftState.byID[result] = entities.dashboards[result]
        const exists = draftState.allIDs.find(id => id === result)

        if (!exists) {
          draftState.allIDs.push(result)
        }

        return
      }

      case EDIT_DASHBOARD: {
        editResource<Dashboard>(draftState, action, ResourceType.Dashboards)

        return
      }

      case REMOVE_CELL: {
        const {dashboardID, cellID} = action

        const {cells} = draftState.byID[dashboardID]

        draftState.byID[dashboardID].cells = cells.filter(
          cell => cell.id !== cellID
        )

        return
      }

      case ADD_DASHBOARD_LABEL: {
        const {dashboardID, label} = action

        draftState.byID[dashboardID].labels.push(label)

        return
      }

      case REMOVE_DASHBOARD_LABEL: {
        const {dashboardID, labelID} = action

        const {labels} = draftState.byID[dashboardID]

        draftState.byID[dashboardID].labels = labels.filter(
          label => label.id !== labelID
        )

        return
      }
    }
  })
}

// Libraries
import {produce} from 'immer'
import _ from 'lodash'

// Types
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
import {Dashboard, RemoteDataState} from 'src/types'

export interface DashboardsState {
  list: Dashboard[]
  status: RemoteDataState
}

const initialState = () => ({
  list: [],
  status: RemoteDataState.NotStarted,
})

export const dashboardsReducer = (
  state: DashboardsState = initialState(),
  action: Action
): DashboardsState => {
  return produce(state, draftState => {
    switch (action.type) {
      case SET_DASHBOARDS: {
        const {list, status} = action.payload

        draftState.status = status
        if (list) {
          draftState.list = list
        }

        return
      }

      case REMOVE_DASHBOARD: {
        const {id} = action.payload
        draftState.list = draftState.list.filter(l => l.id !== id)

        return
      }

      case SET_DASHBOARD: {
        const {dashboard} = action.payload
        draftState.list = _.unionBy([dashboard], state.list, 'id')

        return
      }

      case EDIT_DASHBOARD: {
        const {dashboard} = action.payload

        draftState.list = draftState.list.map(d => {
          if (d.id === dashboard.id) {
            return dashboard
          }
          return d
        })

        return
      }

      case REMOVE_CELL: {
        const {dashboard, cell} = action.payload
        draftState.list = draftState.list.map(d => {
          if (d.id === dashboard.id) {
            const cells = d.cells.filter(c => c.id !== cell.id)
            d.cells = cells
          }

          return d
        })

        return
      }

      case ADD_DASHBOARD_LABEL: {
        const {dashboardID, label} = action.payload

        draftState.list = draftState.list.map(d => {
          if (d.id === dashboardID) {
            d.labels = [...d.labels, label]
          }

          return d
        })

        return
      }

      case REMOVE_DASHBOARD_LABEL: {
        const {dashboardID, label} = action.payload
        draftState.list = draftState.list.map(d => {
          if (d.id === dashboardID) {
            const updatedLabels = d.labels.filter(el => !(label.id === el.id))

            d.labels = updatedLabels
          }

          return d
        })

        return
      }
    }
  })
}

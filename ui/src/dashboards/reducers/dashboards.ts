// Libraries
import {produce} from 'immer'
import _ from 'lodash'

// Types
import {Action, ActionTypes} from 'src/dashboards/actions'
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
      case ActionTypes.SetDashboards: {
        const {list, status} = action.payload

        draftState.status = status
        if (list) {
          draftState.list = list
        }

        return
      }

      case ActionTypes.RemoveDashboard: {
        const {id} = action.payload
        draftState.list = draftState.list.filter(l => l.id !== id)

        return
      }

      case ActionTypes.SetDashboard: {
        const {dashboard} = action.payload
        draftState.list = _.unionBy([dashboard], state.list, 'id')

        return
      }

      case ActionTypes.EditDashboard: {
        const {dashboard} = action.payload

        draftState.list = draftState.list.map(d => {
          if (d.id === dashboard.id) {
            return dashboard
          }
          return d
        })

        return
      }

      case ActionTypes.RemoveCell: {
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

      case ActionTypes.AddDashboardLabel: {
        const {dashboardID, label} = action.payload

        draftState.list = draftState.list.map(d => {
          if (d.id === dashboardID) {
            d.labels = [...d.labels, label]
          }

          return d
        })

        return
      }

      case ActionTypes.RemoveDashboardLabel: {
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

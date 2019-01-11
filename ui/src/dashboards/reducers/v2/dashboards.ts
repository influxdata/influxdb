import {Action, ActionTypes} from 'src/dashboards/actions/v2'
import {Dashboard} from 'src/types/v2'
import _ from 'lodash'

type State = Dashboard[]

export default (state: State = [], action: Action): State => {
  switch (action.type) {
    case ActionTypes.LoadDashboards: {
      const {dashboards} = action.payload

      return [...dashboards]
    }

    case ActionTypes.DeleteDashboard: {
      const {dashboardID} = action.payload

      return [...state.filter(d => d.id !== dashboardID)]
    }

    case ActionTypes.LoadDashboard: {
      const {dashboard} = action.payload

      const newDashboards = _.unionBy([dashboard], state, 'id')

      return newDashboards
    }

    case ActionTypes.UpdateDashboard: {
      const {dashboard} = action.payload
      const newState = state.map(
        d => (d.id === dashboard.id ? {...dashboard} : d)
      )

      return [...newState]
    }

    case ActionTypes.DeleteCell: {
      const {dashboard, cell} = action.payload
      const newState = state.map(d => {
        if (d.id !== dashboard.id) {
          return {...d}
        }

        const cells = d.cells.filter(c => c.id !== cell.id)
        return {...d, cells}
      })

      return [...newState]
    }

    case ActionTypes.AddDashboardLabels: {
      const {dashboardID, labels} = action.payload

      const newState = state.map(d => {
        if (d.id === dashboardID) {
          return {...d, labels: [...d.labels, ...labels]}
        }
        return d
      })

      return [...newState]
    }

    case ActionTypes.RemoveDashboardLabels: {
      const {dashboardID, labels} = action.payload

      const newState = state.map(d => {
        if (d.id === dashboardID) {
          const updatedLabels = d.labels.filter(l => {
            return !labels.includes(l)
          })
          return {...d, labels: updatedLabels}
        }
        return d
      })

      return [...newState]
    }
  }

  return state
}

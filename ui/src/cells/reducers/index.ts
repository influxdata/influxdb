// Libraries
import {produce} from 'immer'
import {get} from 'lodash'

// Actions
import {REMOVE_CELL, SET_CELL, Action} from 'src/cells/actions/creators'
import {
  SET_DASHBOARD,
  Action as DashboardAction,
} from 'src/dashboards/actions/creators'

// Types
import {Cell, ResourceState} from 'src/types'

type CellsState = ResourceState['cells']

const initialState = () => ({
  byID: {},
})

export const cellsReducer = (
  state: CellsState = initialState(),
  action: Action | DashboardAction
) =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_DASHBOARD: {
        const {schema} = action

        if (get(schema, ['entities', 'cells'])) {
          draftState.byID = schema.entities.cells
        }

        return
      }

      case SET_CELL: {
        const {id, schema, status} = action

        const cell: Cell = get(schema, ['entities', 'cells', id])
        const cellExists = !!draftState.byID[id]

        if (cell || !cellExists) {
          draftState.byID[id] = {...cell, status}
        } else {
          draftState.byID[id].status = status
        }

        return
      }

      case REMOVE_CELL: {
        delete draftState.byID[action.id]

        return
      }
    }
  })

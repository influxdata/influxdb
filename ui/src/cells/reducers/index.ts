// Libraries
import {produce} from 'immer'
import {get} from 'lodash'

// Actions
import {
  SET_CELLS,
  REMOVE_CELL,
  SET_CELL,
  Action,
} from 'src/cells/actions/creators'
import {
  SET_DASHBOARD,
  Action as DashboardAction,
} from 'src/dashboards/actions/creators'
import {SET_VIEWS_AND_CELLS} from 'src/views/actions/creators'

// Types
import {Cell, ResourceState, RemoteDataState} from 'src/types'

type CellsState = ResourceState['cells']

const initialState = () => ({
  byID: {},
  status: RemoteDataState.NotStarted,
})

export const cellsReducer = (
  state: CellsState = initialState(),
  action: Action | DashboardAction
) =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_DASHBOARD: {
        const {schema, status} = action

        draftState.status = status

        if (get(schema, ['entities', 'cells'])) {
          draftState.byID = schema.entities.cells
        }

        return
      }

      case SET_CELLS: {
        const {status, schema} = action

        draftState.status = status

        if (get(schema, ['entities', 'cells'])) {
          draftState.byID = schema.entities['cells']
        }

        return
      }

      case SET_VIEWS_AND_CELLS: {
        const {status, cellsArray} = action

        cellsArray.forEach(cellSchema => {
          draftState.status = status

          if (get(cellSchema, ['entities', 'cells'])) {
            draftState.byID = cellSchema.entities['cells']
          }
        })

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

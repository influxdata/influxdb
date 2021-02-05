// Libraries
import {produce} from 'immer'

// Actions
import {
  SET_DASHBOARD_VISIT,
  SET_SCROLL,
  Action,
  SET_CELL_MOUNT,
} from 'src/perf/actions'

export interface PerfState {
  dashboard: {
    scroll: 'not scrolled' | 'scrolled'
    byID: {
      [id: string]: {
        startVisitMs: number
      }
    }
  }
  cells: {
    byID: {
      [id: string]: {
        mountStartMs: number
      }
    }
  }
}

const initialState = (): PerfState => ({
  dashboard: {
    scroll: 'not scrolled',
    byID: {},
  },
  cells: {
    byID: {},
  },
})

const perfReducer = (
  state: PerfState = initialState(),
  action: Action
): PerfState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_SCROLL: {
        const {component, scroll} = action
        draftState[component].scroll = scroll

        return
      }

      case SET_DASHBOARD_VISIT: {
        const {dashboardID, startVisitMs} = action
        const exists = draftState.dashboard.byID[dashboardID]

        if (!exists) {
          draftState.dashboard.byID[dashboardID] = {startVisitMs}

          return
        }

        draftState.dashboard.byID[dashboardID].startVisitMs = startVisitMs

        return
      }

      case SET_CELL_MOUNT: {
        const {cellID, mountStartMs} = action
        const exists = draftState.cells.byID[cellID]

        if (!exists) {
          draftState.cells.byID[cellID] = {mountStartMs}

          return
        }

        draftState.cells.byID[cellID].mountStartMs = mountStartMs

        return
      }
    }
  })

export default perfReducer

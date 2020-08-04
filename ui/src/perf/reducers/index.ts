// Libraries
import {produce} from 'immer'

// Actions
import {
  SET_RENDER_ID,
  SET_SCROLL,
  Action,
  SET_CELL_MOUNT,
} from 'src/perf/actions'

export interface PerfState {
  dashboard: {
    scroll: 'not scrolled' | 'scrolled'
    renderID: string
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
    renderID: '',
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
      case SET_RENDER_ID: {
        const {component, renderID} = action
        draftState[component].renderID = renderID

        return
      }

      case SET_SCROLL: {
        const {component, scroll} = action
        draftState[component].scroll = scroll

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

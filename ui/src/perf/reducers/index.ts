// Libraries
import {produce} from 'immer'

// Actions
import {SET_RENDER_ID, SET_SCROLL, Action} from 'src/perf/actions'

export interface PerfState {
  dashboard: {
    scroll: 'not scrolled' | 'scrolled'
    renderID: string
  }
}

const initialState = (): PerfState => ({
  dashboard: {
    scroll: 'not scrolled',
    renderID: '',
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
    }
  })

export default perfReducer

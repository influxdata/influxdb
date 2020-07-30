// Libraries
import {produce} from 'immer'

// Actions
import {SET_MOUNT_ID, SET_SCROLL, Action} from 'src/perf/actions'

export interface PerfState {
  dashboard: {
    scroll: 'not scrolled' | 'scrolled'
    mountID: string
  }
}

const initialState = (): PerfState => ({
  dashboard: {
    scroll: 'not scrolled',
    mountID: '',
  },
})

const perfReducer = (
  state: PerfState = initialState(),
  action: Action
): PerfState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_MOUNT_ID: {
        const {component, mountID} = action
        draftState[component].mountID = mountID

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

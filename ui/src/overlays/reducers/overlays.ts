// Libraries
import {produce} from 'immer'

// Types
import {ActionTypes, Actions} from 'src/overlays/actions/overlays'

export interface OverlayParams {
  [key: string]: string
}

export interface OverlayState {
  id: string | null
  params: OverlayParams
}

const nullParams = {}

const defaultState: OverlayState = {
  id: null,
  params: nullParams,
}

export const overlaysReducer = (
  state = defaultState,
  action: Actions
): OverlayState =>
  produce(state, draftState => {
    switch (action.type) {
      case ActionTypes.ShowOverlay: {
        const {overlayID, overlayParams} = action.payload
        draftState.id = overlayID
        draftState.params = overlayParams
        return
      }
      case ActionTypes.DismissOverlay: {
        draftState.id = null
        draftState.params = nullParams
        return
      }
    }
  })

export default overlaysReducer

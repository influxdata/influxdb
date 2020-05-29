import {OverlayParams, OverlayID} from 'src/overlays/reducers/overlays'

export enum ActionTypes {
  ShowOverlay = 'SHOW_OVERLAY',
  DismissOverlay = 'DISMISS_OVERLAY',
}

export type Actions = ShowOverlay | DismissOverlay

export interface ShowOverlay {
  type: ActionTypes.ShowOverlay
  payload: {
    overlayID: OverlayID
    overlayParams: OverlayParams
    onClose: () => void
  }
}

export const showOverlay = (
  overlayID: OverlayID,
  overlayParams: OverlayParams,
  onClose: () => void
): ShowOverlay => {
  return {
    type: ActionTypes.ShowOverlay,
    payload: {overlayID, overlayParams, onClose},
  }
}

export interface DismissOverlay {
  type: ActionTypes.DismissOverlay
}

export const dismissOverlay = (): DismissOverlay => {
  return {
    type: ActionTypes.DismissOverlay,
  }
}

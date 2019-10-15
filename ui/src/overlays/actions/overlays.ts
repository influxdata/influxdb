import {OverlayParams} from 'src/overlays/reducers/overlays'

export enum ActionTypes {
  ShowOverlay = 'SHOW_OVERLAY',
  DismissOverlay = 'DISMISS_OVERLAY',
}

export type Actions = ShowOverlay | DismissOverlay

export interface ShowOverlay {
  type: ActionTypes.ShowOverlay
  payload: {
    overlayID: string
    overlayParams: OverlayParams
  }
}

export const showOverlay = (
  overlayID: string,
  overlayParams: OverlayParams
): ShowOverlay => {
  return {
    type: ActionTypes.ShowOverlay,
    payload: {overlayID, overlayParams},
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

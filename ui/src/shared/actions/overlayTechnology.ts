import {ReactElement} from 'react'

type OverlayNodeType = ReactElement<any>

interface Options {
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
  transitionTime?: number
}

export type ShowOverlayActionCreator = (
  OverlayNode: OverlayNodeType,
  options: Options
) => ShowOverlayAction

interface ShowOverlayAction {
  type: 'SHOW_OVERLAY'
  payload: {
    OverlayNode
    options
  }
}

export const showOverlay = (
  OverlayNode: OverlayNodeType,
  options: Options
): ShowOverlayAction => ({
  type: 'SHOW_OVERLAY',
  payload: {OverlayNode, options},
})

export const dismissOverlay = () => ({
  type: 'DISMISS_OVERLAY',
})

import {ReactElement} from 'react'

type OverlayNodeType = ReactElement<any>

interface Options {
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
  transitionTime?: number
}

export type ShowOverlay = (
  OverlayNode: OverlayNodeType,
  options: Options
) => ActionOverlayNode

export interface ActionOverlayNode {
  type: 'SHOW_OVERLAY'
  payload: {
    OverlayNode
    options
  }
}

export const showOverlay = (
  OverlayNode: OverlayNodeType,
  options: Options
) => ({
  type: 'SHOW_OVERLAY',
  payload: {OverlayNode, options},
})

export const dismissOverlay = () => ({
  type: 'DISMISS_OVERLAY',
})

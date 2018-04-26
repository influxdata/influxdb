import {ReactElement} from 'react'

type OverlayNodeType = ReactElement<any>

interface Options {
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
  transitionTime?: number
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

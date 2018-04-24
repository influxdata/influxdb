import {ReactElement} from 'react'

type OverlayNode = ReactElement<any>

interface Options {
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
  transitionTime?: number
}

export const showOverlay = (overlayNode: OverlayNode, options: Options) => ({
  type: 'SHOW_OVERLAY',
  payload: {overlayNode, options},
})

export const dismissOverlay = () => ({
  type: 'DISMISS_OVERLAY',
})

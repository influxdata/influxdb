export const showOverlay = (overlayNode, options) => ({
  type: 'SHOW_OVERLAY',
  payload: {overlayNode, options},
})

export const dismissOverlay = () => ({
  type: 'DISMISS_OVERLAY',
})

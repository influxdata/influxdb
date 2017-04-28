// Trigger resize event to relayout the React Layout plugin

export default function resizeLayout() {
  return next => action => {
    next(action)
    if (
      action.type === 'ENABLE_PRESENTATION_MODE' ||
      action.type === 'DISABLE_PRESENTATION_MODE'
    ) {
      window.dispatchEvent(new Event('resize'))
    }
  }
}

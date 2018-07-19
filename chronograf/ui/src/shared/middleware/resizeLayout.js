// Trigger resize event to relayout the React Layout plugin
export const resizeLayout = () => next => action => {
  next(action)

  if (
    action.type === 'ENABLE_PRESENTATION_MODE' ||
    action.type === 'DISABLE_PRESENTATION_MODE'
  ) {
    // Uses longer event object creation method due to IE compatibility.
    const evt = document.createEvent('HTMLEvents')
    evt.initEvent('resize', false, true)
    window.dispatchEvent(evt)
  }
}

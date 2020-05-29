import {Dispatch, Action, Middleware} from 'redux'

// Trigger resize event to re-layout the React Layout plugin
export const resizeLayout: Middleware = () => (next: Dispatch<Action>) => (
  action: Action
) => {
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

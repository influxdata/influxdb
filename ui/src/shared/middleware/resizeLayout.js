// Trigger resize event to relayout the React Layout plugin
import queryString from 'query-string'
import {enablePresentationMode} from 'src/shared/actions/app'

export default function resizeLayout() {
  return next => action => {
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

    const qs = queryString.parse(window.location.search)

    if (qs.present === 'true') {
      next(enablePresentationMode())
      next(action)
    }
  }
}

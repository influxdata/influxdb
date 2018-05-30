// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'

export const queryStringConfig = () => dispatch => action => {
  dispatch(action)
  const urlQueries = queryString.parse(window.location.search)

  // Presentation Mode
  if (urlQueries.present === 'true') {
    dispatch(enablePresentationMode())
  }
}

// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'

export const queryStringConfig = () => dispatch => action => {
  dispatch(action)
  const queries = queryString.parse(window.location.search)

  // Presentation Mode
  if (queries.present === 'true') {
    dispatch(enablePresentationMode())
  }
}

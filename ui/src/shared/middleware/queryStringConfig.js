// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'

export const queryStringConfig = () => dispatch => action => {
  dispatch(action)
  const urlQueryParams = queryString.parse(window.location.search)

  if (urlQueryParams.present === 'true') {
    dispatch(enablePresentationMode())
  }
}

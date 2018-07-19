// Middleware generally used for actions needing parsed queryStrings
import qs from 'qs'

import {enablePresentationMode} from 'src/shared/actions/app'

export const queryStringConfig = () => dispatch => action => {
  dispatch(action)

  const urlQueryParams = qs.parse(window.location.search, {
    ignoreQueryPrefix: true,
  })

  if (urlQueryParams.present === 'true') {
    dispatch(enablePresentationMode())
  }
}

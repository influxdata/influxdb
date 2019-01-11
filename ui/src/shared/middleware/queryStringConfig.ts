import {Middleware, Dispatch, Action} from 'redux'
// Middleware generally used for actions needing parsed queryStrings
import qs from 'qs'

import {enablePresentationMode} from 'src/shared/actions/app'

export const queryStringConfig: Middleware = () => (
  dispatch: Dispatch<Action>
) => (action: Action) => {
  dispatch(action)

  const urlQueryParams = qs.parse(window.location.search, {
    ignoreQueryPrefix: true,
  })

  if (urlQueryParams.present === 'true') {
    dispatch(enablePresentationMode())
  }
}

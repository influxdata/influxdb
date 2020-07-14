// Libraries
import {get} from 'lodash'

// Types
import {AppState} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

// TODO(desa): is this reasonable or can I trust app state?
const emptyMeState: MeState = {
  id: '',
  name: '',
  links: {
    self: '',
    log: '',
  },
}

export const getMe = (state: AppState): MeState => {
  return get(state, 'me', emptyMeState)
}

// Types
import {AppState} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

export const getMe = (state: AppState): MeState => {
  return state.me
}

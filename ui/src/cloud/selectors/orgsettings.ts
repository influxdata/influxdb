import {AppState} from 'src/types'
import {get} from 'lodash'

export const getOrgSettings = (state: AppState) => {
  return get(state, 'cloud.orgSettings.settings', [])
}

import {UserSettingsState} from 'src/userSettings/reducers'
import {AutoRefreshState} from 'src/shared/reducers/autoRefresh'
import {RangeState} from 'src/dashboards/reducers/ranges'
import {AppState, ResourceState} from 'src/types'

export interface LocalStorage {
  VERSION: string
  app: AppState['app']
  me: AppState['me']
  ranges: RangeState
  autoRefresh: AutoRefreshState
  userSettings: UserSettingsState
  resources: {
    orgs: ResourceState['orgs']
    variables: ResourceState['variables']
  }
}

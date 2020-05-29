import {UserSettingsState} from 'src/userSettings/reducers'
import {AutoRefreshState} from 'src/shared/reducers/autoRefresh'
import {FlagState} from 'src/shared/reducers/flags'
import {RangeState} from 'src/dashboards/reducers/ranges'
import {AppState, ResourceState} from 'src/types'

export interface LocalStorage {
  VERSION: string
  app: AppState['app']
  flags: FlagState
  ranges: RangeState
  autoRefresh: AutoRefreshState
  userSettings: UserSettingsState
  resources: {
    orgs: ResourceState['orgs']
    variables: ResourceState['variables']
  }
}

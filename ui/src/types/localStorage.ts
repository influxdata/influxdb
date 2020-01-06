import {VariablesState} from 'src/variables/reducers'
import {UserSettingsState} from 'src/userSettings/reducers'
import {AutoRefreshState} from 'src/shared/reducers/autoRefresh'
import {RangeState} from 'src/dashboards/reducers/ranges'
import {AppState, ResourceState} from 'src/types'

export interface LocalStorage {
  VERSION: string
  app: AppState['app']
  ranges: RangeState
  autoRefresh: AutoRefreshState
  variables: VariablesState
  userSettings: UserSettingsState
  resources: ResourceState
}

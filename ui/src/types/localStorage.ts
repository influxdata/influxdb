import {AppState} from 'src/shared/reducers/app'
import {VariablesState} from 'src/variables/reducers'
import {UserSettingsState} from 'src/userSettings/reducers'
import {OrgsState} from 'src/organizations/reducers/orgs'
import {AutoRefreshState} from 'src/shared/reducers/autoRefresh'
import {RangeState} from 'src/dashboards/reducers/ranges'

export interface LocalStorage {
  VERSION: string
  app: AppState
  ranges: RangeState
  autoRefresh: AutoRefreshState
  variables: VariablesState
  userSettings: UserSettingsState
  orgs: OrgsState
}

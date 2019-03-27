import {AppState} from 'src/shared/reducers/app'
import {VariablesState} from 'src/variables/reducers'
import {UserSettingsState} from 'src/userSettings/reducers'

export interface LocalStorage {
  VERSION: string
  app: AppState
  ranges: any[]
  variables: VariablesState
  userSettings: UserSettingsState
}

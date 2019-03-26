import {AppState} from 'src/shared/reducers/app'
import {VariablesState} from 'src/variables/reducers'

export interface LocalStorage {
  VERSION: string
  app: AppState
  ranges: any[]
  variables: VariablesState
}

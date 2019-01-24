import {AppState} from 'src/shared/reducers/app'

export interface LocalStorage {
  VERSION: string
  app: AppState
  ranges: any[]
}

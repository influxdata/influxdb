import {TimeRange} from 'src/types'
import {AppState} from 'src/shared/reducers/app'

export interface LocalStorage {
  VERSION: string
  app: AppState
  ranges: []
  timeRange: TimeRange
}

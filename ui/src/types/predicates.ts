import {Filter, RemoteDataState, CustomTimeRange} from 'src/types'

export interface PredicatesState {
  bucketName: string
  deletionStatus: RemoteDataState
  files: string[]
  filters: Filter[]
  isSerious: boolean
  keys: string[]
  previewStatus: RemoteDataState
  timeRange: CustomTimeRange
  values: string[]
}

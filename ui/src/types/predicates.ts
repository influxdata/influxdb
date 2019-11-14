import {Filter, RemoteDataState} from 'src/types'

export interface PredicatesState {
  bucketName: string
  deletionStatus: RemoteDataState
  filters: Filter[]
  isSerious: boolean
  keys: string[]
  timeRange: [number, number]
  values: string[]
}

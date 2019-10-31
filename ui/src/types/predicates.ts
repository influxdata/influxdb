import {Filter, RemoteDataState} from 'src/types'

export interface PredicatesState {
  bucketName: string
  timeRange: [number, number]
  filters: Filter[]
  isSerious: boolean
  deletionStatus: RemoteDataState
}

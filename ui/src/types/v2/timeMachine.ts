import {QueryFn} from 'src/shared/constants/queryBuilder'

export enum TimeMachineTab {
  Queries = 'queries',
  Visualization = 'visualization',
}

export interface BuilderConfig {
  buckets: string[]
  measurements: string[]
  fields: string[]
  functions: QueryFn[]
}

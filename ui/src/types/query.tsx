export interface Query {
  id: QueryID
  database: string
  measurement: string
  retentionPolicy: string
  fields: Field[]
  tags: Tags
  groupBy: GroupBy
  areTagsAccepted: boolean
  rawText: string | null
  range?: TimeRange | null
  source?: string
  fill: string
  status?: Status
}

export interface Field {
  value: string
  type: string
  alias?: string
  args?: Args[]
}

export type QueryID = string

export interface Args {
  value: string
  type: string
  alias?: string
  args?: Args[]
}

export type TagValues = string[]

export interface Tags {
  [key: string]: TagValues
}

export interface GroupBy {
  time?: string
  tags?: string[]
}

export interface Namespace {
  database: string
  retentionPolicy: string
}

export interface Status {
  loading?: string
  error?: string
  warn?: string
  success?: string
}

export interface TimeRange {
  lower: string
  upper?: string
}

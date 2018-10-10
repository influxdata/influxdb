import {Source} from 'src/types'

export interface Query {
  text: string
  id: string
  queryConfig: QueryConfig
}

export interface QueryConfig {
  id?: string
  database?: string
  measurement?: string
  retentionPolicy?: string
  fields?: Field[]
  tags: Tags
  groupBy?: GroupBy
  areTagsAccepted: boolean
  rawText?: string
  range?: DurationRange | null
  source?: Source | null // doesn't come from server -- is set in CellEditorOverlay
  fill?: string
  status?: Status
  shifts?: TimeShift[]
  lower?: string
  upper?: string
  isQuerySupportedByExplorer?: boolean // doesn't come from server -- is set in CellEditorOverlay
}

export interface Field {
  value: string
  type: string
  alias?: string
  args?: FieldArg[]
}

export interface FieldArg {
  value: string
  type: string
  alias?: string
  args?: FieldArg[]
}

export interface FieldFunc extends Field {
  args: FuncArg[]
}
export interface FuncArg {
  type: string
  value: string
  alias?: string
}

export interface ApplyFuncsToFieldArgs {
  field: Field
  funcs: FuncArg[]
}

export interface Tag {
  key: string
  value: string
}

export type TagValues = string[]

export interface Tags {
  [key: string]: TagValues
}

export interface GroupBy {
  time?: string | null
  tags?: string[]
}

export interface Namespace {
  database: string
  retentionPolicy: string
}

export interface Status {
  loading?: boolean
  error?: string
  warn?: string
  success?: string
}

export interface TimeRange {
  lower: string
  upper?: string | null
  seconds?: number
  format?: string
  label?: string
}

export interface DurationRange {
  lower: string
  upper?: string
}

export interface TimeShift {
  label: string
  unit: string
  quantity: string
}

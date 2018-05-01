import {QueryConfig} from 'src/types'
import {ColorString} from 'src/types/colors'

interface Axis {
  bounds: [string, string]
  label: string
  prefix: string
  suffix: string
  base: string
  scale: string
}

export interface Axes {
  x: Axis
  y: Axis
}

interface FieldName {
  internalName: string
  displayName: string
  visible: boolean
}

export interface TableOptions {
  timeFormat: string
  verticalTimeAxis: boolean
  sortBy: FieldName
  wrapping: string
  fixFirstColumn: boolean
}

interface CellLinks {
  self: string
}

export interface CellQuery {
  query: string
  queryConfig: QueryConfig
}

export interface Legend {
  type?: string
  orientation?: string
}

interface DecimalPlaces {
  isEnforced: boolean
  digits: number
}

export interface Cell {
  id: string
  x: number
  y: number
  w: number
  h: number
  name: string
  queries: CellQuery[]
  type: string
  axes: Axes
  colors: ColorString[]
  tableOptions: TableOptions
  fieldOptions: FieldName[]
  timeFormat: string
  decimalPlaces: DecimalPlaces
  links: CellLinks
  legend: Legend
}

interface TemplateValue {
  value: string
  selected?: boolean
}

export interface Template {
  id: string
  tempVar: string
  values: TemplateValue[]
}

export type CellEditorOverlayActionsFunc = (id: string, ...args: any[]) => any

export interface CellEditorOverlayActions {
  chooseNamespace: (id: string) => void
  chooseMeasurement: (id: string) => void
  applyFuncsToField: (id: string) => void
  chooseTag: (id: string) => void
  groupByTag: (id: string) => void
  toggleField: (id: string) => void
  groupByTime: (id: string) => void
  toggleTagAcceptance: (id: string) => void
  fill: (id: string) => void
  editRawTextAsync: (url: string, id: string, text: string) => Promise<void>
  addInitialField: (id: string) => void
  removeFuncs: (id: string) => void
  timeShift: (id: string) => void
}

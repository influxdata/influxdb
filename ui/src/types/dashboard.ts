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

interface Axes {
  x: Axis
  y: Axis
}

interface FieldName {
  internalName: string
  displayName: string
  visible: boolean
}

interface TableOptions {
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
  chooseNamespace: CellEditorOverlayActionsFunc
  chooseMeasurement: CellEditorOverlayActionsFunc
  applyFuncsToField: CellEditorOverlayActionsFunc
  chooseTag: CellEditorOverlayActionsFunc
  groupByTag: CellEditorOverlayActionsFunc
  toggleField: CellEditorOverlayActionsFunc
  groupByTime: CellEditorOverlayActionsFunc
  toggleTagAcceptance: CellEditorOverlayActionsFunc
  fill: CellEditorOverlayActionsFunc
  editRawTextAsync: CellEditorOverlayActionsFunc
  addInitialField: CellEditorOverlayActionsFunc
  removeFuncs: CellEditorOverlayActionsFunc
  timeShift: CellEditorOverlayActionsFunc
}

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

export interface FieldName {
  internalName: string
  displayName: string
  visible: boolean
}

export interface TableOptions {
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

export interface DecimalPlaces {
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
  type: CellType
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

export enum CellType {
  Line = 'line',
  Stacked = 'line-stacked',
  StepPlot = 'line-stepplot',
  Bar = 'bar',
  LinePlusSingleStat = 'line-plus-single-stat',
  SingleStat = 'single-stat',
  Gauge = 'gauge',
  Table = 'table',
}

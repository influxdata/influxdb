import {Query} from 'src/types'
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
  precision: number
}

interface TableOptions {
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
  queryConfig: Query
}

export interface Legend {
  type?: string
  orientation?: string
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

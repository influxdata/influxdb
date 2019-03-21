import {
  ILabel,
  Variable,
  IDashboard,
  DocumentListEntry,
  Document,
} from '@influxdata/influx'
import {View, Cell} from 'src/types/v2'

export enum TemplateType {
  Label = 'label',
  Task = 'task',
  Dashboard = 'dashboard',
  View = 'view',
  Cell = 'cell',
  Variable = 'variable',
}

interface KeyValuePairs {
  [key: string]: any
}

// Templates
interface TemplateBase extends Document {
  content: {data: TemplateData; included: TemplateIncluded[]}
  labels?: ILabel[]
}

// TODO: be more specific about what attributes can be
interface TemplateData {
  type: TemplateType
  attributes: KeyValuePairs
  relationships: {[key in TemplateType]?: {data: IRelationship[]}}
}

interface TemplateIncluded {
  type: TemplateType
  id: string
  attributes: KeyValuePairs
}

// Template Relationships
type IRelationship =
  | CellRelationship
  | LabelRelationship
  | ViewRelationship
  | VariableRelationship

export interface CellRelationship {
  type: TemplateType.Cell
  id: string
}

export interface LabelRelationship {
  type: TemplateType.Label
  id: string
}

export interface VariableRelationship {
  type: TemplateType.Variable
  id: string
}

interface ViewRelationship {
  type: TemplateType.View
  id: string
}

// Template Includeds
export interface ViewIncluded extends TemplateIncluded {
  type: TemplateType.View
  attributes: View
}

export interface CellIncluded extends TemplateIncluded {
  type: TemplateType.Cell
  attributes: Cell
  relationships: {
    [TemplateType.View]: {data: ViewRelationship}
  }
}

export interface LabelIncluded extends TemplateIncluded {
  type: TemplateType.Label
  attributes: ILabel
}

export interface VariableIncluded extends TemplateIncluded {
  type: TemplateType.Variable
  attributes: Variable
}

export type TaskTemplateIncluded = LabelIncluded

export type DashboardTemplateIncluded =
  | CellIncluded
  | ViewIncluded
  | LabelIncluded
  | VariableIncluded

// Template Datas
interface TaskTemplateData extends TemplateData {
  type: TemplateType.Task
  attributes: {name: string; flux: string}
  relationships: {
    [TemplateType.Label]: {data: LabelRelationship[]}
  }
}

interface DashboardTemplateData extends TemplateData {
  type: TemplateType.Dashboard
  attributes: IDashboard
  relationships: {
    [TemplateType.Label]: {data: LabelRelationship[]}
    [TemplateType.Cell]: {data: CellRelationship[]}
    [TemplateType.Variable]: {data: VariableRelationship[]}
  }
}

// Templates
export interface TaskTemplate extends TemplateBase {
  content: {
    data: TaskTemplateData
    included: TaskTemplateIncluded[]
  }
}

export interface DashboardTemplate extends TemplateBase {
  content: {
    data: DashboardTemplateData
    included: DashboardTemplateIncluded[]
  }
}

export type Template = TaskTemplate | DashboardTemplate

export interface TemplateSummary extends DocumentListEntry {
  labels: ILabel[]
}

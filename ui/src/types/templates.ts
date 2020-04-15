import {
  Document,
  DocumentCreate,
  DocumentMeta,
  TemplateSummary as GenTemplateSummary,
  TemplateType,
} from '@influxdata/influx'
import {
  Cell,
  Dashboard,
  NormalizedState,
  RemoteDataState,
  Variable,
  View,
  GenLabel,
  PostVariable,
  Label,
} from 'src/types'

export interface TemplateSummary extends Omit<GenTemplateSummary, 'labels'> {
  labels: string[]
  status: RemoteDataState
}

export interface TemplatesState extends NormalizedState<TemplateSummary> {
  exportTemplate: {status: RemoteDataState; item: DocumentCreate}
}

interface KeyValuePairs {
  [key: string]: any
}

interface DocumentMetaWithTemplateID extends DocumentMeta {
  templateID?: string
}

// Templates
export interface GenTemplateBase extends Document {
  meta: DocumentMetaWithTemplateID
  content: {data: TemplateData; included: TemplateIncluded[]}
  labels: Label[]
}

export interface TemplateBase extends Omit<Document, 'labels'> {
  meta: DocumentMetaWithTemplateID
  content: {data: TemplateData; included: TemplateIncluded[]}
  labels: string[]
}

// TODO: be more specific about what attributes can be
interface TemplateData {
  type: TemplateType
  attributes: KeyValuePairs
  relationships: Relationships
}

interface TemplateIncluded {
  type: TemplateType
  id: string
  attributes: KeyValuePairs
  relationships?: Relationships
}

// enforces key association with relationship type
export type Relationships = {
  [key in keyof RelationshipMap]?: {
    data: OneOrMany<RelationshipMap[key]>
  }
}

type OneOrMany<T> = T | T[]

interface RelationshipMap {
  [TemplateType.Cell]: CellRelationship
  [TemplateType.Label]: LabelRelationship
  [TemplateType.View]: ViewRelationship
  [TemplateType.Variable]: VariableRelationship
}

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
  attributes: GenLabel
}

export interface VariableIncluded extends TemplateIncluded {
  type: TemplateType.Variable
  attributes: Variable
  relationships: {
    [TemplateType.Label]: {data: LabelRelationship[]}
  }
}

export type TaskTemplateIncluded = LabelIncluded

export type DashboardTemplateIncluded =
  | CellIncluded
  | ViewIncluded
  | LabelIncluded
  | VariableIncluded

export type VariableTemplateIncluded = LabelIncluded | VariableIncluded

// Template Data
interface TaskTemplateData extends TemplateData {
  type: TemplateType.Task
  attributes: {name: string; flux: string}
  relationships: {
    [TemplateType.Label]: {data: LabelRelationship[]}
  }
}

interface DashboardTemplateData extends TemplateData {
  type: TemplateType.Dashboard
  attributes: Dashboard
  relationships: {
    [TemplateType.Label]: {data: LabelRelationship[]}
    [TemplateType.Cell]: {data: CellRelationship[]}
    [TemplateType.Variable]: {data: VariableRelationship[]}
  }
}

interface VariableTemplateData extends TemplateData {
  type: TemplateType.Variable
  attributes: PostVariable
  relationships: {
    [TemplateType.Label]: {data: LabelRelationship[]}
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

export interface VariableTemplate extends TemplateBase {
  content: {
    data: VariableTemplateData
    included: VariableTemplateIncluded[]
  }
}

export type Template = TaskTemplate | DashboardTemplate | VariableTemplate

export {TemplateType} from '@influxdata/influx'

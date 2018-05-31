import {Service} from 'src/types'
// function definitions
export type OnDeleteFuncNode = (ids: DeleteFuncNodeArgs) => void
export type OnChangeArg = (inputArg: InputArg) => void
export type OnAddNode = (
  bodyID: string,
  funcName: string,
  declarationID: string
) => void
export type OnGenerateScript = (script: string) => void
export type OnChangeScript = (script: string) => void
export type OnSubmitScript = () => void

export interface ScriptStatus {
  type: string
  text: string
}

export interface Context {
  onAddNode: OnAddNode
  onChangeArg: OnChangeArg
  onSubmitScript: OnSubmitScript
  onChangeScript: OnChangeScript
  onDeleteFuncNode: OnDeleteFuncNode
  onGenerateScript: OnGenerateScript
  service: Service
}

export interface DeleteFuncNodeArgs {
  funcID: string
  bodyID: string
  declarationID?: string
}

export interface InputArg {
  funcID: string
  bodyID: string
  declarationID?: string
  key: string
  value: string | boolean
  generate?: boolean
}

// Flattened AST
export interface BinaryExpressionNode {
  source: string
  type: string
}

interface ObjectNode {
  name: string
  type: string
}

interface PropertyNode {
  name?: string
  value?: string
  type: string
}

export interface MemberExpressionNode {
  type: string
  source: string
  object: ObjectNode
  property: PropertyNode
}

export interface FlatBody {
  type: string
  source: string
  funcs?: Func[]
  declarations?: FlatDeclaration[]
}

export interface Func {
  type: string
  name: string
  args: Arg[]
  source: string
  id: string
}

type Value = string | boolean

export interface Arg {
  key: string
  value: Value
  type: string
}

interface FlatExpression {
  id: string
  funcs?: Func[]
}

interface FlatDeclaration extends FlatExpression {
  name: string
  value: string
  type: string
}

// Semantic Graph list of available functions for ifql queries
export interface Suggestion {
  name: string
  params: {
    [key: string]: string
  }
}

export interface Links {
  self: string
  suggestions: string
  ast: string
}

// FluxTable is the result of a request to IFQL
// https://github.com/influxdata/platform/blob/master/query/docs/SPEC.md#response-format
export interface FluxTable {
  id: string
  name: string
  data: string[][]
  partitionKey: {
    [key: string]: string
  }
}

export interface SchemaFilter {
  key: string
  value: string
}

export enum RemoteDataState {
  NotStarted = 'NotStarted',
  Loading = 'Loading',
  Done = 'Done',
  Error = 'Error',
}

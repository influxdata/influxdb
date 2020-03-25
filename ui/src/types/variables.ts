import {Variable as GenVariable, Label} from 'src/client'
import {VariableProperties as GenVariableProperties} from 'src/client'

import {
  VariableArgumentType,
  QueryArguments,
  MapArguments,
  CSVArguments,
  RemoteDataState,
  NormalizedState,
} from 'src/types'

// GenVariable is the shape of a variable from the server
export type GenVariable = GenVariable
export interface SystemVariableProperties {
  type?: 'system'
  values?: any
}
export type VariableProperties =
  | SystemVariableProperties
  | GenVariableProperties

export interface Variable
  extends Omit<Omit<GenVariable, 'labels'>, 'arguments'> {
  status: RemoteDataState // Loading status of an individual variable
  labels: string[]
  arguments: VariableProperties
}

export interface PostVariable extends GenVariable {
  labels: Label[]
}

export type FluxColumnType =
  | 'boolean'
  | 'unsignedLong'
  | 'long'
  | 'double'
  | 'string'
  | 'base64Binary'
  | 'dateTime'
  | 'duration'

export type mapValue = string
export interface VariableMapObject {
  [mapKey: string]: mapValue
}
export interface VariableValues {
  values?: VariableMapObject | string[]
  valueType?: FluxColumnType
  selected?: string[]
  error?: string
}

export interface VariableValuesByID {
  [variableID: string]: VariableValues
}

export interface ValueSelections {
  [variableID: string]: string
}

export interface VariablesState extends NormalizedState<Variable> {
  values: {
    // Different variable values can be selected in different
    // "contexts"---different parts of the app like a particular dashboard, or
    // the Data Explorer
    [contextID: string]: {
      status: RemoteDataState
      order: string[] // IDs of variables
      values: VariableValuesByID
    }
  }
}

export interface VariableEditorState {
  name: string
  selected: VariableArgumentType
  argsQuery: QueryArguments
  argsMap: MapArguments
  argsConstant: CSVArguments
}

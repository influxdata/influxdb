// Types
import {
  VariableEntities,
  VariableArgumentType,
  RemoteDataState,
  QueryArguments,
  MapArguments,
  CSVArguments,
  VariableValuesByID,
} from 'src/types'
import {NormalizedSchema} from 'normalizr'

export const SET_VARIABLES = 'SET_VARIABLES'
export const SET_VARIABLE = 'SET_VARIABLE'
export const REMOVE_VARIABLE = 'REMOVE_VARIABLE'

export type Action =
  | ReturnType<typeof setVariables>
  | ReturnType<typeof setVariable>
  | ReturnType<typeof removeVariable>
  | ReturnType<typeof moveVariable>
  | ReturnType<typeof setValues>
  | ReturnType<typeof selectValue>

// R is the type of the value of the "result" key in normalizr's normalization
type VariablesSchema<R extends string | string[]> = NormalizedSchema<
  VariableEntities,
  R
>

export const setVariables = (
  status: RemoteDataState,
  schema?: VariablesSchema<string[]>
) =>
  ({
    type: SET_VARIABLES,
    status,
    schema,
  } as const)

export const setVariable = (
  id: string,
  status: RemoteDataState,
  schema?: VariablesSchema<string>
) =>
  ({
    type: SET_VARIABLE,
    id,
    status,
    schema,
  } as const)

export const removeVariable = (id: string) =>
  ({
    type: REMOVE_VARIABLE,
    id,
  } as const)

export const moveVariable = (
  originalIndex: number,
  newIndex: number,
  contextID: string
) => ({
  type: 'MOVE_VARIABLE' as 'MOVE_VARIABLE',
  payload: {originalIndex, newIndex, contextID},
})

export const setValues = (
  contextID: string,
  status: RemoteDataState,
  values?: VariableValuesByID
) => ({
  type: 'SET_VARIABLE_VALUES' as 'SET_VARIABLE_VALUES',
  payload: {contextID, status, values},
})

export const selectValue = (
  contextID: string,
  variableID: string,
  selectedValue: string
) => ({
  type: 'SELECT_VARIABLE_VALUE' as 'SELECT_VARIABLE_VALUE',
  payload: {contextID, variableID, selectedValue},
})

// Variable Editor Action Creators
export type EditorAction =
  | ReturnType<typeof clearEditor>
  | ReturnType<typeof updateType>
  | ReturnType<typeof updateName>
  | ReturnType<typeof updateQuery>
  | ReturnType<typeof updateMap>
  | ReturnType<typeof updateConstant>

export const clearEditor = () => ({
  type: 'CLEAR_VARIABLE_EDITOR' as 'CLEAR_VARIABLE_EDITOR',
})

export const updateType = (type: VariableArgumentType) => ({
  type: 'CHANGE_VARIABLE_EDITOR_TYPE' as 'CHANGE_VARIABLE_EDITOR_TYPE',
  payload: type,
})

export const updateName = (name: string) => ({
  type: 'UPDATE_VARIABLE_EDITOR_NAME' as 'UPDATE_VARIABLE_EDITOR_NAME',
  payload: name,
})

export const updateQuery = (arg: QueryArguments) => ({
  type: 'UPDATE_VARIABLE_EDITOR_QUERY' as 'UPDATE_VARIABLE_EDITOR_QUERY',
  payload: arg,
})

export const updateMap = (arg: MapArguments) => ({
  type: 'UPDATE_VARIABLE_EDITOR_MAP' as 'UPDATE_VARIABLE_EDITOR_MAP',
  payload: arg,
})

export const updateConstant = (arg: CSVArguments) => ({
  type: 'UPDATE_VARIABLE_EDITOR_CONSTANT' as 'UPDATE_VARIABLE_EDITOR_CONSTANT',
  payload: arg,
})

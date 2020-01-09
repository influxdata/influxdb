// Types
import {
  VariableArgumentType,
  RemoteDataState,
  QueryArguments,
  MapArguments,
  CSVArguments,
  Variable,
  VariableValuesByID,
} from 'src/types'

export type Action =
  | ReturnType<typeof setVariables>
  | ReturnType<typeof setVariable>
  | ReturnType<typeof removeVariable>
  | ReturnType<typeof moveVariable>
  | ReturnType<typeof setValues>
  | ReturnType<typeof selectValue>

export const setVariables = (
  status: RemoteDataState,
  variables?: Variable[]
) => ({
  type: 'SET_VARIABLES' as 'SET_VARIABLES',
  payload: {status, variables},
})

export const setVariable = (
  id: string,
  status: RemoteDataState,
  variable?: Variable
) => ({
  type: 'SET_VARIABLE' as 'SET_VARIABLE',
  payload: {id, status, variable},
})

export const removeVariable = (id: string) => ({
  type: 'REMOVE_VARIABLE' as 'REMOVE_VARIABLE',
  payload: {id},
})

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

// API
import {client} from 'src/utils/api'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  getVariablesFailed,
  getVariableFailed,
  createVariableFailed,
  updateVariableFailed,
  deleteVariableFailed,
} from 'src/shared/copy/notifications'

// Types
import {Dispatch} from 'redux-thunk'
import {RemoteDataState} from 'src/types'
import {Variable} from '@influxdata/influx'

export type Action = SetVariables | SetVariable | RemoveVariable

interface SetVariables {
  type: 'SET_VARIABLES'
  payload: {
    status: RemoteDataState
    variables?: Variable[]
  }
}

const setVariables = (
  status: RemoteDataState,
  variables?: Variable[]
): SetVariables => ({
  type: 'SET_VARIABLES',
  payload: {status, variables},
})

interface SetVariable {
  type: 'SET_VARIABLE'
  payload: {
    id: string
    status: RemoteDataState
    variable?: Variable
  }
}

const setVariable = (
  id: string,
  status: RemoteDataState,
  variable?: Variable
): SetVariable => ({
  type: 'SET_VARIABLE',
  payload: {id, status, variable},
})

interface RemoveVariable {
  type: 'REMOVE_VARIABLE'
  payload: {id: string}
}

const removeVariable = (id: string): RemoveVariable => ({
  type: 'REMOVE_VARIABLE',
  payload: {id},
})

export const getVariables = () => async (dispatch: Dispatch<Action>) => {
  try {
    dispatch(setVariables(RemoteDataState.Loading))

    const variables = await client.variables.getAll()

    dispatch(setVariables(RemoteDataState.Done, variables))
  } catch (e) {
    console.log(e)
    dispatch(setVariables(RemoteDataState.Error))
    dispatch(notify(getVariablesFailed()))
  }
}

export const getVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const variable = await client.variables.get(id)

    dispatch(setVariable(id, RemoteDataState.Done, variable))
  } catch (e) {
    console.log(e)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(getVariableFailed()))
  }
}

export const createVariable = (variable: Variable) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const createdVariable = await client.variables.create(variable)

    dispatch(
      setVariable(createdVariable.id, RemoteDataState.Done, createdVariable)
    )
  } catch (e) {
    console.log(e)
    dispatch(notify(createVariableFailed()))
  }
}

export const updateVariable = (id: string, props: Partial<Variable>) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const variable = await client.variables.update(id, props)

    dispatch(setVariable(id, RemoteDataState.Done, variable))
  } catch (e) {
    console.log(e)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(updateVariableFailed()))
  }
}

export const deleteVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    await client.variables.delete(id)

    dispatch(removeVariable(id))
  } catch (e) {
    console.log(e)
    dispatch(setVariable(id, RemoteDataState.Done))
    dispatch(notify(deleteVariableFailed()))
  }
}

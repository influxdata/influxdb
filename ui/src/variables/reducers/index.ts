// Libraries
import {produce} from 'immer'
import {get} from 'lodash'

// Types
import {RemoteDataState, VariablesState} from 'src/types'
import {Action} from 'src/variables/actions/creators'

export const initialState = (): VariablesState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
  values: {},
})

export const variablesReducer = (
  state: VariablesState = initialState(),
  action: Action
): VariablesState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_VARIABLES': {
        const {status, variables} = action.payload

        draftState.status = status

        if (variables) {
          draftState.byID = {}

          for (const variable of variables) {
            draftState.byID[variable.id] = {
              ...variable,
              status: RemoteDataState.Done,
            }
          }
        }

        return
      }

      case 'SET_VARIABLE': {
        const {id, status, variable} = action.payload
        const variableExists = !!draftState.byID[id]

        if (variable || !variableExists) {
          draftState.byID[id] = {...variable, status}
        } else {
          draftState.byID[id].status = status
        }

        return
      }

      case 'REMOVE_VARIABLE': {
        const {id} = action.payload

        delete draftState.byID[id]

        return
      }

      case 'SET_VARIABLE_VALUES': {
        const {contextID, status, values} = action.payload
        const prevOrder = get(draftState, `values.${contextID}.order`, [])

        if (values) {
          const order = Object.keys(values).sort(
            (a, b) => prevOrder.indexOf(a) - prevOrder.indexOf(b)
          )

          draftState.values[contextID] = {
            status,
            values,
            order,
          }
        } else if (draftState.values[contextID]) {
          draftState.values[contextID].status = status
        } else {
          draftState.values[contextID] = {status, values: null, order: []}
        }

        return
      }

      case 'SELECT_VARIABLE_VALUE': {
        const {contextID, variableID, selectedValue} = action.payload

        const valuesExist = !!get(
          draftState,
          `values.${contextID}.values.${variableID}`
        )

        if (!valuesExist) {
          return
        }

        draftState.values[contextID].values[
          variableID
        ].selectedValue = selectedValue

        return
      }

      case 'MOVE_VARIABLE': {
        const {originalIndex, newIndex, contextID} = action.payload

        const variableIDToMove = get(
          draftState,
          `values.${contextID}.order[${originalIndex}]`
        )

        const variableIDToSwap = get(
          draftState,
          `values.${contextID}.order[${newIndex}]`
        )

        draftState.values[contextID].order[originalIndex] = variableIDToSwap
        draftState.values[contextID].order[newIndex] = variableIDToMove

        return
      }
    }
  })

export {variableEditorReducer} from 'src/variables/reducers/editor'

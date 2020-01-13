// Libraries
import {produce} from 'immer'
import {get} from 'lodash'

// Types
import {
  Variable,
  RemoteDataState,
  VariablesState,
  ResourceType,
} from 'src/types'
import {
  Action,
  SET_VARIABLES,
  SET_VARIABLE,
  REMOVE_VARIABLE,
  MOVE_VARIABLE,
  SET_VARIABLE_VALUES,
  SELECT_VARIABLE_VALUE,
} from 'src/variables/actions/creators'

// Utils
import {setResource, removeResource} from 'src/resources/reducers/helpers'

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
      case SET_VARIABLES: {
        setResource<Variable>(draftState, action, ResourceType.Variables)

        return
      }

      case SET_VARIABLE: {
        const {id, status, schema} = action
        const {entities} = schema

        const variable = get(entities, ['variables', id])
        const variableExists = !!draftState.byID[id]

        if (variable || !variableExists) {
          draftState.byID[id] = {...variable, status}
          draftState.allIDs.push(id)
        } else {
          draftState.byID[id].status = status
        }

        return
      }

      case REMOVE_VARIABLE: {
        removeResource<Variable>(draftState, action)

        return
      }

      case SET_VARIABLE_VALUES: {
        const {contextID, status, values} = action
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

      case SELECT_VARIABLE_VALUE: {
        const {contextID, variableID, selectedValue} = action

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

      case MOVE_VARIABLE: {
        const {originalIndex, newIndex, contextID} = action

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

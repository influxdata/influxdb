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

        const variable = get(schema, ['entities', 'variables', id])
        const variableExists = !!draftState.byID[id]

        if (variable) {
          draftState.byID[id] = {...variable, status}

          if (!variableExists) {
            draftState.allIDs.push(id)
          }
        } else {
          draftState.byID[id].status = status
        }

        return
      }

      case REMOVE_VARIABLE: {
        removeResource<Variable>(draftState, action)

        return
      }

      case SELECT_VARIABLE_VALUE: {
        const {contextID, variableID, selectedValue} = action

        if (!draftState.values[contextID]) {
          draftState.values[contextID] = {
            status: RemoteDataState.Done,
            order: draftState.allIDs,
            values: {},
          }
        }

        if (!draftState.values[contextID].values[variableID]) {
          draftState.values[contextID].values[variableID] = {
            selected: [selectedValue],
          }

          return
        }

        draftState.values[contextID].values[variableID].selected = [
          selectedValue,
        ]

        return
      }

      case MOVE_VARIABLE: {
        const {originalIndex, newIndex, contextID} = action
        let newOrder = get(draftState, `values.${contextID}.order`)

        // if no order, take it from allIDs
        if (!newOrder) {
          newOrder = get(draftState, `allIDs`)
        }

        newOrder = newOrder.slice(0)

        const idToMove = newOrder[originalIndex]
        const idToSwap = newOrder[newIndex]

        newOrder[originalIndex] = idToSwap
        newOrder[newIndex] = idToMove

        draftState.values[contextID] = {
          ...(draftState.values[contextID] || {
            status: RemoteDataState.NotStarted,
            values: {},
          }),
          order: newOrder,
        }

        return
      }
    }
  })

export {variableEditorReducer} from 'src/variables/reducers/editor'

// Libraries
import {get} from 'lodash'

// Types
import {ResourceType} from 'src/types'

export const setResource = (draftState, action, resource: ResourceType) => {
  const {status, schema} = action

  draftState.status = status

  if (get(schema, 'entities')) {
    draftState.byID = schema.entities[resource]
    draftState.allIDs = schema.result
  }

  return
}

export const addResource = (draftState, action, resource: ResourceType) => {
  const {result, entities} = action.schema
  const bucket = entities[resource][result]

  draftState.byID[result] = bucket
  draftState.allIDs.push(result)
}

export const editResource = (draftState, action, resource: ResourceType) => {
  const {entities, result} = action.schema
  const bucket = entities[resource][result]
  draftState.byID[result] = bucket
}

export const removeResource = (draftState, action) => {
  const {id} = action
  delete draftState.byID[id]
  draftState.allIDs = draftState.allIDs.filter(uuid => uuid !== id)

  return
}

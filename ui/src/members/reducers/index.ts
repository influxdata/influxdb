// Libraries
import {get} from 'lodash'
import {produce} from 'immer'

// Types
import {RemoteDataState, ResourceState} from 'src/types'
import {
  Action,
  SET_MEMBERS,
  ADD_MEMBER,
  REMOVE_MEMBER,
} from 'src/members/actions/creators'

export type MembersState = ResourceState['members']

const initialState = (): MembersState => ({
  byID: null,
  allIDs: [],
  status: RemoteDataState.NotStarted,
})

export const membersReducer = (
  state: MembersState = initialState(),
  action: Action
): MembersState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_MEMBERS: {
        const {status, schema} = action
        draftState.status = status
        if (get(schema, 'entities')) {
          draftState.byID = schema.entities.members
          draftState.allIDs = schema.result
        }

        return
      }

      case ADD_MEMBER: {
        const {result, entities} = action.schema
        const bucket = entities.members[result]

        draftState.byID[result] = bucket
        draftState.allIDs.push(result)

        return
      }

      case REMOVE_MEMBER: {
        const {id} = action

        delete draftState.byID[id]
        draftState.allIDs = draftState.allIDs.filter(uuid => uuid !== id)

        return
      }
    }
  })

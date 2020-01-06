// Libraries
import {produce} from 'immer'

// Types
import {Member, RemoteDataState, ResourceState, ResourceType} from 'src/types'
import {
  Action,
  SET_MEMBERS,
  ADD_MEMBER,
  REMOVE_MEMBER,
} from 'src/members/actions/creators'

// Utils
import {
  setResource,
  addResource,
  removeResource,
} from 'src/resources/reducers/helpers'

const {Members} = ResourceType
export type MembersState = ResourceState['members']

const initialState = (): MembersState => ({
  byID: {},
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
        setResource<Member>(draftState, action, Members)

        return
      }

      case ADD_MEMBER: {
        addResource<Member>(draftState, action, Members)

        return
      }

      case REMOVE_MEMBER: {
        removeResource<Member>(draftState, action)

        return
      }
    }
  })

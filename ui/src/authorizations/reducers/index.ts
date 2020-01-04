// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, ResourceState, ResourceType} from 'src/types'
import {
  Action,
  SET_AUTH,
  ADD_AUTH,
  EDIT_AUTH,
  REMOVE_AUTH,
} from 'src/authorizations/actions/creators'

// Utils
import {
  setResource,
  addResource,
  removeResource,
  editResource,
} from 'src/shared/reducers/helpers'

type AuthsState = ResourceState['tokens']
const {Authorizations} = ResourceType

const initialState = (): AuthsState => ({
  status: RemoteDataState.NotStarted,
  byID: null,
  allIDs: [],
})

export const authsReducer = (
  state: AuthsState = initialState(),
  action: Action
): AuthsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_AUTH: {
        setResource(draftState, action, Authorizations)

        return
      }

      case ADD_AUTH: {
        addResource(draftState, action, Authorizations)

        return
      }

      case REMOVE_AUTH: {
        removeResource(draftState, action)

        return
      }

      case EDIT_AUTH: {
        editResource(draftState, action, Authorizations)

        return
      }
    }
  })

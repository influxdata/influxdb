// Libraries
import {produce} from 'immer'

// Types
import {
  RemoteDataState,
  ResourceState,
  ResourceType,
  Authorization,
} from 'src/types'
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
} from 'src/resources/reducers/helpers'

type AuthsState = ResourceState['tokens']
const {Authorizations} = ResourceType

const initialState = (): AuthsState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
})

export const authsReducer = (
  state: AuthsState = initialState(),
  action: Action
): AuthsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_AUTH: {
        setResource<Authorization>(draftState, action, Authorizations)

        return
      }

      case ADD_AUTH: {
        addResource<Authorization>(draftState, action, Authorizations)

        return
      }

      case REMOVE_AUTH: {
        removeResource<Authorization>(draftState, action)

        return
      }

      case EDIT_AUTH: {
        editResource<Authorization>(draftState, action, Authorizations)

        return
      }
    }
  })

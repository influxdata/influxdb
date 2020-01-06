// Libraries
import {produce} from 'immer'

// Types
import {
  SET_ORGS,
  SET_ORG,
  ADD_ORG,
  REMOVE_ORG,
  Action,
  EDIT_ORG,
} from 'src/organizations/actions/creators'
import {
  ResourceState,
  Organization,
  ResourceType,
  RemoteDataState,
} from 'src/types'

// Utils
import {
  setResource,
  addResource,
  removeResource,
  editResource,
} from 'src/resources/reducers/helpers'

const {Orgs} = ResourceType
type OrgsState = ResourceState['orgs']

const initialState = (): OrgsState => ({
  byID: {},
  allIDs: [],
  status: RemoteDataState.NotStarted,
  org: null,
})

export const orgsReducer = (
  state: OrgsState = initialState(),
  action: Action
) =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_ORGS: {
        setResource<Organization>(draftState, action, Orgs)

        return
      }
      case ADD_ORG: {
        addResource<Organization>(draftState, action, Orgs)

        return
      }

      case REMOVE_ORG: {
        removeResource<Organization>(draftState, action)

        return
      }

      case EDIT_ORG: {
        editResource<Organization>(draftState, action, Orgs)

        return
      }

      case SET_ORG: {
        draftState.org = action.org

        return
      }
    }
  })

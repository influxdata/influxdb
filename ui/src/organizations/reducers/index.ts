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

        // NOTE: this is a normalization issue because the current org
        // is being updated, but the selected org object isn't being
        // updated. Since we have only one org at a time in this app
        // i've taken some liberties here
        if (action.schema.result === draftState.org.id) {
          draftState.org = action.schema.entities.orgs[action.schema.result]
        }

        return
      }

      case SET_ORG: {
        draftState.org = action.org

        return
      }
    }
  })

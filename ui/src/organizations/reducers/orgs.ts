// Libraries
import {produce} from 'immer'

// Types
import {ActionTypes, Actions} from 'src/organizations/actions/orgs'
import {Organization, RemoteDataState} from 'src/types'

export interface OrgsState {
  status: RemoteDataState
  items: Organization[]
}

const defaultState: OrgsState = {
  status: RemoteDataState.NotStarted,
  items: [],
}

export const orgsReducer = (state = defaultState, action: Actions): OrgsState =>
  produce(state, draftState => {
    switch (action.type) {
      case ActionTypes.SetOrgsStatus: {
        const {status} = action.payload
        draftState.status = status
        return
      }
      case ActionTypes.SetOrgs: {
        const {status, orgs} = action.payload
        draftState.status = status || draftState.status
        draftState.items = orgs || draftState.items
        return
      }
      case ActionTypes.AddOrg: {
        const {org} = action.payload
        draftState.items.push(org)
        return
      }
      case ActionTypes.RemoveOrg: {
        const {org} = action.payload
        draftState.items = draftState.items.filter(o => o.id !== org.id)
        return
      }
      case ActionTypes.EditOrg: {
        const {org} = action.payload
        draftState.items = draftState.items.map(o => {
          if (o.id === org.id) {
            return {...o, ...org}
          }
          return o
        })
        return
      }
      default:
        return
    }
  })

export default orgsReducer

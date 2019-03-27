import {ActionTypes, Actions} from 'src/organizations/actions/orgs'
import {Organization} from 'src/types'

const defaultState = []

export default (state = defaultState, action: Actions): Organization[] => {
  switch (action.type) {
    case ActionTypes.SetOrgs:
      return [...action.payload.organizations]
    case ActionTypes.AddOrg:
      return [...state, {...action.payload.org}]
    case ActionTypes.RemoveOrg:
      return state.filter(org => org !== action.payload.org)
    case ActionTypes.EditOrg:
      const newState = state.map(o => {
        const {org} = action.payload
        if (o.links.self === org.links.self) {
          return {...o, ...org}
        }

        return o
      })

      return newState
    default:
      return state
  }
}

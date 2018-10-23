import {Organization} from 'src/types/v2'
import {ActionTypes, Actions} from 'src/organizations/actions'

const defaultState = []

export default (state = defaultState, action: Actions): Organization[] => {
  switch (action.type) {
    case ActionTypes.SetOrgs:
      return [...action.payload.organizations]
    case ActionTypes.AddOrg:
      return [...state, {...action.payload.org}]
    case ActionTypes.RemoveOrg:
      return state.filter(org => org.links.self !== action.payload.link)
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

import {Organization} from 'src/shared/apis/v2/organization'
import {ActionTypes, Actions} from 'src/shared/actions/v2/orgs'

const defaultState = []

export default (state = defaultState, action: Actions): Organization[] => {
  switch (action.type) {
    case ActionTypes.SetOrganizations:
      return action.payload.organizations
    default:
      return state
  }
}

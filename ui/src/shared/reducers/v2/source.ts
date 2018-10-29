import {Source} from 'src/types/v2'

import {Actions, ActionTypes} from 'src/shared/actions/v2/source'

export type SourceState = Source

const defaultState: SourceState = {
  name: '',
  id: '',
  type: '',
  url: '',
  insecureSkipVerify: false,
  default: false,
  telegraf: '',
  links: null,
}

export default (state = defaultState, action: Actions): SourceState => {
  switch (action.type) {
    case ActionTypes.ResetSource:
      return {...defaultState}
    case ActionTypes.SetSource:
      return {...action.payload.source}
    default:
      return state
  }
}

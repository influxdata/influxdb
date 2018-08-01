import {Action, ActionTypes} from 'src/shared/actions/links'
import {Links} from 'src/types/v2/links'

const initialState: Links = {
  sources: '',
}

const linksReducer = (state = initialState, action: Action): Links => {
  switch (action.type) {
    case ActionTypes.LinksGetCompleted: {
      const {links} = action.payload
      return {...links}
    }
  }

  return state
}

export default linksReducer

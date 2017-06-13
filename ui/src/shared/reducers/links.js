import * as actionTypes from 'shared/constants/actionTypes'

const initialState = {
  external: {statusFeed: ''},
}

const linksReducer = (state = initialState, action) => {
  switch (action.type) {
    case actionTypes.LINKS_RECEIVED: {
      const {links} = action.payload

      return links
    }

    default: {
      return state
    }
  }
}

export default linksReducer

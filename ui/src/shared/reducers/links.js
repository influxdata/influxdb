import * as actionTypes from 'shared/constants/actionTypes'

const initialState = {
  external: {statusFeed: ''},
  custom: [],
}

const linksReducer = (state = initialState, action) => {
  console.log(action.type, actionTypes.LINKS_GET_COMPLETED)
  switch (action.type) {
    case actionTypes.LINKS_GET_COMPLETED: {
      const {links} = action.payload
      console.log('linksReducer', links)
      return links
    }

    default: {
      return state
    }
  }
}

export default linksReducer

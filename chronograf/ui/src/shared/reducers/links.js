const initialState = {
  external: {statusFeed: ''},
  custom: [],
}

const linksReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'LINKS_GET_COMPLETED': {
      const {links} = action.payload
      return {...links}
    }
  }

  return state
}

export default linksReducer

const getInitialState = () => ({
  error: null,
})

const initialState = getInitialState()

const errorsReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'ERROR_THROWN': {
      return {...action.error}
    }
  }

  return state
}

export default errorsReducer

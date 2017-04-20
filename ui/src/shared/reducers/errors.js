const getInitialState = () => ({
  error: null,
})

export const initialState = getInitialState()

const errorsReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'ERROR_THROWN': {
      const {error} = action
      return {error}
    }
  }

  return state
}

export default errorsReducer

const initialState = [
  {
    id: '0',
    group: '',
    name: 'anno1',
    time: '1515716169000',
    duration: '33600000', // 1 hour
    text: 'you have no swoggels',
  },
  {
    id: '1',
    group: '',
    name: 'anno2',
    time: '1515772377000',
    duration: '',
    text: 'another annotation',
  },
]

const annotationsReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'LOAD_ANNOTATIONS': {
      return action.payload.annotations
    }

    case 'UPDATE_ANNOTATION': {
      const {annotation} = action.payload
      const newState = state.map(a => (a.id === annotation.id ? annotation : a))

      return newState
    }
  }

  return state
}

export default annotationsReducer

const initialState = [
  {
    group: '',
    name: 'anno1',
    time: '1515716169000',
    duration: '33600000', // 1 hour
    text: 'you have no swoggels',
  },
  {
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
  }

  return state
}

export default annotationsReducer

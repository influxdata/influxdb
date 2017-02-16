const initialState = {
  presentationMode: false,
};

export default function ui(state = initialState, action) {
  switch (action.type) {
    case 'ENABLE_PRESENTATION_MODE': {
      return {
        ...state,
        presentationMode: true,
      }
    }

    case 'DISABLE_PRESENTATION_MODE': {
      return {
        ...state,
        presentationMode: false,
      }
    }
  }

  return state
}

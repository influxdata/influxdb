const initialState = {
  options: {
    dismissOnClickOutside: false,
    dismissOnEscape: false,
  },
  overlayNode: null,
}

export default function overlayTechnology(state = initialState, action) {
  switch (action.type) {
    case 'SHOW_OVERLAY': {
      const {overlayNode, options} = action.payload

      return {...state, overlayNode, options}
    }

    case 'DISMISS_OVERLAY': {
      return {
        ...state,
        overlayNode: null,
        options: null,
      }
    }
  }

  return state
}

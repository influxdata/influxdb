const initialState = {
  options: {
    dismissOnClickOutside: false,
    dismissOnEscape: false,
    transitionTime: 300,
  },
  OverlayNode: null,
}

export default function overlayTechnology(state = initialState, action) {
  switch (action.type) {
    case 'SHOW_OVERLAY': {
      const {OverlayNode, options} = action.payload

      return {...state, OverlayNode, options}
    }

    case 'DISMISS_OVERLAY': {
      const {options} = initialState
      return {
        ...state,
        OverlayNode: null,
        options,
      }
    }
  }

  return state
}

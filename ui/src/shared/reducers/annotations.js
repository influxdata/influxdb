import {DEFAULT_ANNOTATION_ID} from 'src/shared/constants/annotations'
import {ADDING, TEMP_ANNOTATION} from 'src/shared/annotations/helpers'

const initialState = {
  mode: null,
  annotations: [
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
  ],
}

const annotationsReducer = (state = initialState, action) => {
  switch (action.type) {
    case 'ADDING_ANNOTATION': {
      return {
        ...state,
        mode: ADDING,
        annotations: [...state.annotations, TEMP_ANNOTATION],
      }
    }

    case 'ADDING_ANNOTATION_SUCCESS': {
      const annotations = state.annotations.filter(
        a => a.id !== TEMP_ANNOTATION.id
      )

      return {...state, mode: null, annotations}
    }

    case 'LOAD_ANNOTATIONS': {
      const {annotations} = action.payload

      return {...state, annotations}
    }

    case 'UPDATE_ANNOTATION': {
      const {annotation} = action.payload
      const annotations = state.annotations.map(
        a => (a.id === annotation.id ? annotation : a)
      )

      return {...state, annotations}
    }

    case 'DELETE_ANNOTATION': {
      const {annotation} = action.payload
      const annotations = state.annotations.filter(a => a.id !== annotation.id)

      return {...state, annotations}
    }

    case 'ADD_ANNOTATION': {
      const {annotation} = action.payload
      const annotations = [
        ...state.annotations,
        {...annotation, id: DEFAULT_ANNOTATION_ID},
      ]

      return {...state, annotations}
    }
  }

  return state
}

export default annotationsReducer

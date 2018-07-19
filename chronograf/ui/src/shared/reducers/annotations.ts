import {ADDING, EDITING, TEMP_ANNOTATION} from 'src/shared/annotations/helpers'

import {Action} from 'src/types/actions/annotations'
import {AnnotationInterface} from 'src/types'

export interface AnnotationState {
  mode: string
  isTempHovering: boolean
  annotations: AnnotationInterface[]
}

const initialState = {
  mode: null,
  isTempHovering: false,
  annotations: [],
}

const annotationsReducer = (
  state: AnnotationState = initialState,
  action: Action
) => {
  switch (action.type) {
    case 'EDITING_ANNOTATION': {
      return {
        ...state,
        mode: EDITING,
      }
    }

    case 'DISMISS_EDITING_ANNOTATION': {
      return {
        ...state,
        mode: null,
      }
    }

    case 'ADDING_ANNOTATION': {
      const annotations = state.annotations.filter(
        a => a.id !== TEMP_ANNOTATION.id
      )

      return {
        ...state,
        mode: ADDING,
        isTempHovering: true,
        annotations: [...annotations, TEMP_ANNOTATION],
      }
    }

    case 'ADDING_ANNOTATION_SUCCESS': {
      return {
        ...state,
        isTempHovering: false,
        mode: null,
      }
    }

    case 'DISMISS_ADDING_ANNOTATION': {
      const annotations = state.annotations.filter(
        a => a.id !== TEMP_ANNOTATION.id
      )

      return {
        ...state,
        isTempHovering: false,
        mode: null,
        annotations,
      }
    }

    case 'MOUSEENTER_TEMP_ANNOTATION': {
      const newState = {
        ...state,
        isTempHovering: true,
      }

      return newState
    }

    case 'MOUSELEAVE_TEMP_ANNOTATION': {
      const newState = {
        ...state,
        isTempHovering: false,
      }

      return newState
    }

    case 'LOAD_ANNOTATIONS': {
      const {annotations} = action.payload

      return {
        ...state,
        annotations,
      }
    }

    case 'UPDATE_ANNOTATION': {
      const {annotation} = action.payload
      const annotations = state.annotations.map(
        a => (a.id === annotation.id ? annotation : a)
      )

      return {
        ...state,
        annotations,
      }
    }

    case 'DELETE_ANNOTATION': {
      const {annotation} = action.payload
      const annotations = state.annotations.filter(a => a.id !== annotation.id)

      return {
        ...state,
        annotations,
      }
    }

    case 'ADD_ANNOTATION': {
      const {annotation} = action.payload
      const annotations = [...state.annotations, annotation]

      return {
        ...state,
        annotations,
      }
    }
  }

  return state
}

export default annotationsReducer

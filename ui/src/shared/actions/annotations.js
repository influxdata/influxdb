import * as api from 'shared/apis/annotation'

export const editingAnnotation = () => ({
  type: 'EDITING_ANNOTATION',
})

export const dismissEditingAnnotation = () => ({
  type: 'DISMISS_EDITING_ANNOTATION',
})

export const addingAnnotation = () => ({
  type: 'ADDING_ANNOTATION',
})

export const addingAnnotationSuccess = () => ({
  type: 'ADDING_ANNOTATION_SUCCESS',
})

export const dismissAddingAnnotation = () => ({
  type: 'DISMISS_ADDING_ANNOTATION',
})

export const mouseEnterTempAnnotation = () => ({
  type: 'MOUSEENTER_TEMP_ANNOTATION',
})

export const mouseLeaveTempAnnotation = () => ({
  type: 'MOUSELEAVE_TEMP_ANNOTATION',
})

export const loadAnnotations = annotations => ({
  type: 'LOAD_ANNOTATIONS',
  payload: {
    annotations,
  },
})

export const updateAnnotation = annotation => ({
  type: 'UPDATE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const deleteAnnotation = annotation => ({
  type: 'DELETE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotation = annotation => ({
  type: 'ADD_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotationAsync = (createUrl, annotation) => async dispatch => {
  dispatch(addAnnotation(annotation))
  const savedAnnotation = await api.createAnnotation(createUrl, annotation)
  dispatch(addAnnotation(savedAnnotation))
  dispatch(deleteAnnotation(annotation))
}

export const getAnnotationsAsync = (indexUrl, since) => async dispatch => {
  const annotations = await api.getAnnotations(indexUrl, since)
  annotations.forEach(a => dispatch(addAnnotation(a)))
}

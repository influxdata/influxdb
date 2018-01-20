export const addingAnnotation = () => ({
  type: 'ADDING_ANNOTATION',
})

export const addingAnnotationSuccess = () => ({
  type: 'ADDING_ANNOTATION_SUCCESS',
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

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

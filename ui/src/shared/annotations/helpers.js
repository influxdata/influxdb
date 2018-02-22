export const ADDING = 'adding'
export const EDITING = 'editing'

export const TEMP_ANNOTATION = {
  id: 'tempAnnotation',
  name: 'New Annotation',
  text: '',
  type: '',
  startTime: '',
  endTime: '',
}

export const getAnnotations = (graph, annotations = []) => {
  const [xStart, xEnd] = graph.xAxisRange()

  if (xStart === 0 && xEnd === 0) {
    return []
  }

  return annotations.filter(a => {
    if (a.endTime === a.startTime) {
      return xStart <= +a.startTime && +a.startTime <= xEnd
    }

    return !(+a.endTime < xStart || xEnd < +a.startTime)
  })
}

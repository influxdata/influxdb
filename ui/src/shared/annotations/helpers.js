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
  if (!graph) {
    return []
  }

  const [xStart, xEnd] = graph.xAxisRange()

  return annotations.filter(a => {
    if (a.endTime === a.startTime) {
      return xStart <= +a.startTime && +a.startTime <= xEnd
    }

    return xStart < +a.endTime || +a.startTime < xEnd
  })
}

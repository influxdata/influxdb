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
    const annoStart = +a.startTime
    const annoEnd = +a.endTime

    // If annotation is too far left
    if (annoEnd < xStart) {
      return false
    }

    // If annotation is too far right
    if (annoStart > xEnd) {
      return false
    }

    return true
  })
}

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
  return annotations.reduce((acc, a) => {
    // Don't render if annotation.time is outside the graph
    const annoStart = +a.startTime
    const annoEnd = +a.endTime
    const endAnnotation = {
      ...a,
      id: `${a.id}-end`,
      startTime: `${annoEnd}`,
      endTime: `${annoEnd}`,
    }

    if (annoStart < xStart) {
      if (annoEnd > xStart) {
        return [...acc, a, endAnnotation]
      }

      return acc
    }

    if (annoStart > xEnd) {
      return acc
    }

    // If annotation does not have duration, include in array
    if (!annoEnd) {
      return [...acc, a]
    }

    // If annoEnd is out of bounds, just render the start point
    if (annoEnd > xEnd) {
      return [...acc, a]
    }

    // Render both the start and end point
    return [...acc, a, endAnnotation]
  }, [])
}

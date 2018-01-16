export const getAnnotations = (graph, annotations) => {
  if (!graph) {
    return []
  }

  const [xStart, xEnd] = graph.xAxisRange()
  return annotations.reduce((acc, a) => {
    // Don't render if annotation.time is outside the graph
    if (+a.time < xStart || +a.time > xEnd) {
      return acc
    }
    // If annotation does not have duration, include in array
    if (!a.duration) {
      return [...acc, a]
    }

    const annotationEndpoint = {
      ...a,
      time: String(Number(a.time) + Number(a.duration)),
      duration: '',
    }

    const endpointOutOfBounds =
      +annotationEndpoint.time < xStart || +annotationEndpoint.time > xEnd

    // If endpoint is out of bounds, just render the start point
    if (endpointOutOfBounds) {
      return [...acc, a]
    }

    // Render both the start and end point
    return [...acc, a, annotationEndpoint]
  }, [])
}

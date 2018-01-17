export const getAnnotations = (graph, annotations = []) => {
  if (!graph) {
    return []
  }

  const [xStart, xEnd] = graph.xAxisRange()
  return annotations.reduce((acc, a) => {
    // Don't render if annotation.time is outside the graph
    const time = +a.time
    const duration = +a.duration

    if (time < xStart) {
      const endPoint = time + duration
      if (endPoint > xStart) {
        return [...acc, a, {...a, time: `${endPoint}`, duration: ''}]
      }

      return acc
    }

    if (time > xEnd) {
      return acc
    }

    // If annotation does not have duration, include in array
    if (!duration) {
      return [...acc, a]
    }

    const annotationEndpoint = {
      ...a,
      time: `${time + duration}`,
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

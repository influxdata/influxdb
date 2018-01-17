export const getAnnotations = (graph, annotations = []) => {
  if (!graph) {
    return []
  }

  const [xStart, xEnd] = graph.xAxisRange()
  return annotations.reduce((acc, a) => {
    // Don't render if annotation.time is outside the graph
    const time = +a.time
    const duration = +a.duration
    const endpoint = time + duration

    if (time < xStart) {
      if (endpoint > xStart) {
        return [...acc, a, {...a, time: `${endpoint}`, duration: ''}]
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

    // If endpoint is out of bounds, just render the start point
    if (endpoint > xEnd) {
      return [...acc, a]
    }

    // Render both the start and end point
    return [...acc, a, {...a, time: `${time + duration}`, duration: ''}]
  }, [])
}

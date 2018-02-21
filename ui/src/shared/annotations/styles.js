const containerLeftPadding = 16

export const annotationLeft = (time, dygraph) => {
  const left = `${dygraph.toDomXCoord(time) + containerLeftPadding}px`
  return {left}
}

export const newCrosshairLeft = left => ({
  left: `${left}px`,
})

export const windowDimensions = (anno, dygraph) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()

  const startTime = Math.max([+anno.startTime, startX])
  const endTime = Math.min([+anno.endTime, endX])

  const windowStartXCoord = dygraph.toDomXCoord(startTime)
  const windowEndXCoord = dygraph.toDomXCoord(endTime)

  const windowWidth = windowEndXCoord - windowStartXCoord
  const isDurationNegative = windowWidth < 0
  const foo = isDurationNegative ? windowWidth : 0

  const left = `${windowStartXCoord + containerLeftPadding + foo}px`
  const width = `${Math.abs(windowWidth)}px`

  return {
    left,
    width,
  }
}

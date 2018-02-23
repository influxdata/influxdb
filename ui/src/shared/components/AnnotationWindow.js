import React, {PropTypes} from 'react'

import * as schema from 'shared/schemas'

const containerLeftPadding = 16

const windowDimensions = (anno, dygraph) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()
  const startTime = Math.max(+anno.startTime, startX)
  const endTime = Math.min(+anno.endTime, endX)

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

const AnnotationWindow = ({annotation, dygraph, active}) =>
  <div
    className={`annotation-window${active ? ' active' : ''}`}
    style={windowDimensions(annotation, dygraph)}
  />

const {bool, shape} = PropTypes

AnnotationWindow.propTypes = {
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  active: bool,
}

export default AnnotationWindow

import React, {PropTypes} from 'react'

import {DYGRAPH_CONTAINER_MARGIN} from 'shared/constants'
import * as schema from 'shared/schemas'

const windowDimensions = (anno, dygraph) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()
  const startTime = Math.max(+anno.startTime, startX)
  const endTime = Math.min(+anno.endTime, endX)

  const windowStartXCoord = dygraph.toDomXCoord(startTime)
  const windowEndXCoord = dygraph.toDomXCoord(endTime)
  const windowWidth = Math.abs(windowEndXCoord - windowStartXCoord)

  const windowLeftXCoord =
    Math.min(windowStartXCoord, windowEndXCoord) + DYGRAPH_CONTAINER_MARGIN

  return {
    left: `${windowLeftXCoord}px`,
    width: `${windowWidth}px`,
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

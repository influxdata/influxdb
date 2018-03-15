import React from 'react'
import PropTypes from 'prop-types'

import {
  DYGRAPH_CONTAINER_H_MARGIN,
  DYGRAPH_CONTAINER_V_MARGIN,
  DYGRAPH_CONTAINER_XLABEL_MARGIN,
} from 'shared/constants'
import * as schema from 'shared/schemas'

const windowDimensions = (anno, dygraph, staticLegendHeight) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()
  const startTime = Math.max(+anno.startTime, startX)
  const endTime = Math.min(+anno.endTime, endX)

  const windowStartXCoord = dygraph.toDomXCoord(startTime)
  const windowEndXCoord = dygraph.toDomXCoord(endTime)
  const windowWidth = Math.abs(windowEndXCoord - windowStartXCoord)

  const windowLeftXCoord =
    Math.min(windowStartXCoord, windowEndXCoord) + DYGRAPH_CONTAINER_H_MARGIN

  const height = staticLegendHeight
    ? `calc(100% - ${staticLegendHeight +
        DYGRAPH_CONTAINER_XLABEL_MARGIN +
        DYGRAPH_CONTAINER_V_MARGIN * 2}px)`
    : 'calc(100% - 36px)'

  return {
    left: `${windowLeftXCoord}px`,
    width: `${windowWidth}px`,
    height,
  }
}

const AnnotationWindow = ({annotation, dygraph, active, staticLegendHeight}) =>
  <div
    className={`annotation-window${active ? ' active' : ''}`}
    style={windowDimensions(annotation, dygraph, staticLegendHeight)}
  />

const {bool, number, shape} = PropTypes

AnnotationWindow.propTypes = {
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  staticLegendHeight: number,
  active: bool,
}

export default AnnotationWindow

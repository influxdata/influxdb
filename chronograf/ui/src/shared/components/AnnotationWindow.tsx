import React from 'react'

import {
  DYGRAPH_CONTAINER_H_MARGIN,
  DYGRAPH_CONTAINER_V_MARGIN,
  DYGRAPH_CONTAINER_XLABEL_MARGIN,
} from 'src/shared/constants'
import {AnnotationInterface, DygraphClass} from 'src/types'

interface WindowDimensionsReturn {
  left: string
  width: string
  height: string
}

const windowDimensions = (
  anno: AnnotationInterface,
  dygraph: DygraphClass,
  staticLegendHeight: number
): WindowDimensionsReturn => {
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

interface AnnotationWindowProps {
  annotation: AnnotationInterface
  dygraph: DygraphClass
  active: boolean
  staticLegendHeight: number
}

const AnnotationWindow = ({
  annotation,
  dygraph,
  active,
  staticLegendHeight,
}: AnnotationWindowProps): JSX.Element => (
  <div
    className={`annotation-window${active ? ' active' : ''}`}
    style={windowDimensions(annotation, dygraph, staticLegendHeight)}
  />
)

export default AnnotationWindow

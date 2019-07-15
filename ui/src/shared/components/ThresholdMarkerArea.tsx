// Libraries
import React, {FunctionComponent} from 'react'

// Types
import {CheckStatusLevel} from 'src/types'

interface Props {
  level: CheckStatusLevel
  top: number
  height: number
}

const ThresholdMarkerArea: FunctionComponent<Props> = ({
  level,
  top,
  height,
}) => (
  <div
    className={`threshold-marker--area threshold-marker--${level.toLowerCase()}`}
    style={{
      top: `${top}px`,
      height: `${height}px`,
    }}
  />
)

export default ThresholdMarkerArea

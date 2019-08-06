// Libraries
import React, {FunctionComponent} from 'react'
import {Scale} from '@influxdata/giraffe'

// Components
import ThresholdMarker from 'src/shared/components/ThresholdMarker'
import ThresholdMarkerArea from 'src/shared/components/ThresholdMarkerArea'

// Utils
import {isInDomain, clamp} from 'src/shared/utils/vis'
import {DragEvent} from 'src/shared/utils/useDragBehavior'

// Types
import {Threshold} from 'src/types'

interface Props {
  yScale: Scale<number, number>
  yDomain: number[]
  threshold: Threshold
  onChangePos: (e: DragEvent) => void
}

const GreaterThresholdMarker: FunctionComponent<Props> = ({
  yDomain,
  yScale,
  threshold: {level, lowerBound},
  onChangePos,
}) => {
  const y = yScale(clamp(lowerBound, yDomain))

  return (
    <>
      {isInDomain(lowerBound, yDomain) && (
        <ThresholdMarker level={level} y={y} onDrag={onChangePos} />
      )}
      {lowerBound <= yDomain[1] && (
        <ThresholdMarkerArea
          level={level}
          top={yScale(yDomain[1])}
          height={y - yScale(yDomain[1])}
        />
      )}
    </>
  )
}

export default GreaterThresholdMarker

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
  onChangeMaxPos: (e: DragEvent) => void
  onChangeMinPos: (e: DragEvent) => void
}

const RangeThresholdMarkers: FunctionComponent<Props> = ({
  yScale,
  yDomain,
  threshold: {level, lowerBound, upperBound},
  onChangeMinPos,
  onChangeMaxPos,
}) => {
  const minY = yScale(clamp(lowerBound, yDomain))
  const maxY = yScale(clamp(upperBound, yDomain))

  return (
    <>
      {isInDomain(lowerBound, yDomain) && (
        <ThresholdMarker level={level} y={minY} onDrag={onChangeMinPos} />
      )}
      {isInDomain(upperBound, yDomain) && (
        <ThresholdMarker level={level} y={maxY} onDrag={onChangeMaxPos} />
      )}
      {lowerBound > upperBound ? (
        <ThresholdMarkerArea level={level} top={maxY} height={minY - maxY} />
      ) : (
        <>
          {upperBound <= yDomain[1] && (
            <ThresholdMarkerArea
              level={level}
              top={yScale(yDomain[1])}
              height={maxY - yScale(yDomain[1])}
            />
          )}
          {lowerBound >= yDomain[0] && (
            <ThresholdMarkerArea
              level={level}
              top={minY}
              height={yScale(yDomain[0]) - minY}
            />
          )}
        </>
      )}
    </>
  )
}

export default RangeThresholdMarkers

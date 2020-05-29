// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'
import {Scale} from '@influxdata/giraffe'

// Components
import ThresholdMarker from 'src/shared/components/ThresholdMarker'
import ThresholdMarkerArea from 'src/shared/components/ThresholdMarkerArea'

// Utils
import {isInDomain, clamp} from 'src/shared/utils/vis'
import {DragEvent} from 'src/shared/utils/useDragBehavior'

// Types
import {RangeThreshold} from 'src/types'

interface Props {
  yScale: Scale<number, number>
  yDomain: number[]
  threshold: RangeThreshold
  onChangeMaxPos: (e: DragEvent) => void
  onChangeMinPos: (e: DragEvent) => void
  onMouseUp: (e: MouseEvent<HTMLDivElement>) => void
}

const RangeThresholdMarkers: FunctionComponent<Props> = ({
  yScale,
  yDomain,
  threshold: {level, within, min, max},
  onChangeMinPos,
  onChangeMaxPos,
  onMouseUp,
}) => {
  const minY = yScale(clamp(min, yDomain))
  const maxY = yScale(clamp(max, yDomain))

  return (
    <>
      {isInDomain(min, yDomain) && (
        <ThresholdMarker
          level={level}
          y={minY}
          onDrag={onChangeMinPos}
          onMouseUp={onMouseUp}
        />
      )}
      {isInDomain(max, yDomain) && (
        <ThresholdMarker
          level={level}
          y={maxY}
          onDrag={onChangeMaxPos}
          onMouseUp={onMouseUp}
        />
      )}
      {within ? (
        <ThresholdMarkerArea level={level} top={maxY} height={minY - maxY} />
      ) : (
        <>
          {max <= yDomain[1] && (
            <ThresholdMarkerArea
              level={level}
              top={yScale(yDomain[1])}
              height={maxY - yScale(yDomain[1])}
            />
          )}
          {min >= yDomain[0] && (
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

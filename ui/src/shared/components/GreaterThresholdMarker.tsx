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
import {GreaterThreshold} from 'src/types'

interface Props {
  yScale: Scale<number, number>
  yDomain: number[]
  threshold: GreaterThreshold
  onChangePos: (e: DragEvent) => void
  onMouseUp: (e: MouseEvent<HTMLDivElement>) => void
}

const GreaterThresholdMarker: FunctionComponent<Props> = ({
  yDomain,
  yScale,
  threshold: {level, value},
  onChangePos,
  onMouseUp,
}) => {
  const y = yScale(clamp(value, yDomain))

  return (
    <>
      {isInDomain(value, yDomain) && (
        <ThresholdMarker
          level={level}
          y={y}
          onDrag={onChangePos}
          onMouseUp={onMouseUp}
        />
      )}
      {value <= yDomain[1] && (
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

// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'
import {Scale} from '@influxdata/giraffe'

// Components
import ThresholdMarker from 'src/shared/components/ThresholdMarker'
import ThresholdMarkerArea from 'src/shared/components/ThresholdMarkerArea'

// Utils
import {clamp, isInDomain} from 'src/shared/utils/vis'
import {DragEvent} from 'src/shared/utils/useDragBehavior'

// Types
import {LesserThreshold} from 'src/types'

interface Props {
  yScale: Scale<number, number>
  yDomain: number[]
  threshold: LesserThreshold
  onChangePos: (e: DragEvent) => void
  onMouseUp: (e: MouseEvent<HTMLDivElement>) => void
}

const LessThresholdMarker: FunctionComponent<Props> = ({
  yScale,
  yDomain,
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
      {value >= yDomain[0] && (
        <ThresholdMarkerArea
          level={level}
          top={y}
          height={yScale(yDomain[0]) - y}
        />
      )}
    </>
  )
}

export default LessThresholdMarker

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
import {GreaterThresholdConfig} from 'src/types'

interface Props {
  yScale: Scale<number, number>
  yDomain: number[]
  threshold: GreaterThresholdConfig
  onChangePos: (e: DragEvent) => void
}

const GreaterThresholdMarker: FunctionComponent<Props> = ({
  yDomain,
  yScale,
  threshold: {level, value},
  onChangePos,
}) => {
  const y = yScale(clamp(value, yDomain))

  return (
    <>
      {isInDomain(value, yDomain) && (
        <ThresholdMarker level={level} y={y} onDrag={onChangePos} />
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

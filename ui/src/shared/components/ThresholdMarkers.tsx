// Libraries
import React, {useRef, FunctionComponent} from 'react'
import {Scale} from '@influxdata/giraffe'

// Components
import RangeThresholdMarkers from 'src/shared/components/RangeThresholdMarkers'
import LessThresholdMarker from 'src/shared/components/LessThresholdMarker'
import GreaterThresholdMarker from 'src/shared/components/GreaterThresholdMarker'

// Utils
import {clamp} from 'src/shared/utils/vis'

// Types
import {Threshold} from 'src/types'

interface Props {
  thresholds: Threshold[]
  onSetThresholds: (newThresholds: Threshold[]) => void
  yScale: Scale<number, number>
  yDomain: number[]
}

const ThresholdMarkers: FunctionComponent<Props> = ({
  yScale,
  yDomain,
  thresholds,
  onSetThresholds,
}) => {
  const originRef = useRef<HTMLDivElement>(null)

  const handleDrag = (index: number, field: string, y: number) => {
    const yRelative = y - originRef.current.getBoundingClientRect().top
    const yValue = clamp(yScale.invert(yRelative), yDomain)
    const nextThreshold: Threshold = {
      ...thresholds[index],
      [field]: yValue,
    }

    const nextThresholds = thresholds.map((t, i) =>
      i === index ? nextThreshold : t
    )

    onSetThresholds(nextThresholds)
  }

  return (
    <div className="threshold-markers" ref={originRef}>
      {thresholds.map((threshold, index) => {
        const onChangePos = ({y}) => handleDrag(index, 'value', y)
        const onChangeMaxPos = ({y}) => handleDrag(index, 'maxValue', y)
        const onChangeMinPos = ({y}) => handleDrag(index, 'minValue', y)

        if (threshold.lowerBound && threshold.upperBound) {
          return (
            <RangeThresholdMarkers
              key={index}
              yScale={yScale}
              yDomain={yDomain}
              threshold={threshold}
              onChangeMinPos={onChangeMinPos}
              onChangeMaxPos={onChangeMaxPos}
            />
          )
        }

        if (threshold.lowerBound) {
          return (
            <GreaterThresholdMarker
              key={index}
              yScale={yScale}
              yDomain={yDomain}
              threshold={threshold}
              onChangePos={onChangePos}
            />
          )
        }

        if (threshold.upperBound) {
          return (
            <LessThresholdMarker
              key={index}
              yScale={yScale}
              yDomain={yDomain}
              threshold={threshold}
              onChangePos={onChangePos}
            />
          )
        }
      })}
    </div>
  )
}

export default ThresholdMarkers

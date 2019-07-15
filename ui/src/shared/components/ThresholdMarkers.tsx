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
import {ThresholdConfig} from 'src/types'

interface Props {
  thresholds: ThresholdConfig[]
  onSetThresholds: (newThresholds: ThresholdConfig[]) => void
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
    const nextThreshold = {...thresholds[index], [field]: yValue}

    if (
      nextThreshold.type === 'range' &&
      nextThreshold.minValue > nextThreshold.maxValue
    ) {
      // If the user drags the min past the max or vice versa, we swap the
      // values that are set so that the min is always at most the max
      const maxValue = nextThreshold.minValue

      nextThreshold.minValue = nextThreshold.maxValue
      nextThreshold.maxValue = maxValue
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

        switch (threshold.type) {
          case 'greater':
            return (
              <GreaterThresholdMarker
                key={index}
                yScale={yScale}
                yDomain={yDomain}
                threshold={threshold}
                onChangePos={onChangePos}
              />
            )
          case 'less':
            return (
              <LessThresholdMarker
                key={index}
                yScale={yScale}
                yDomain={yDomain}
                threshold={threshold}
                onChangePos={onChangePos}
              />
            )
          case 'range':
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
          default:
            return null
        }
      })}
    </div>
  )
}

export default ThresholdMarkers

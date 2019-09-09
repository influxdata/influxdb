// Libraries
import React, {useRef, FunctionComponent} from 'react'
import {Scale} from '@influxdata/giraffe'
import {round} from 'lodash'

// Components
import RangeThresholdMarkers from 'src/shared/components/RangeThresholdMarkers'
import LessThresholdMarker from 'src/shared/components/LessThresholdMarker'
import GreaterThresholdMarker from 'src/shared/components/GreaterThresholdMarker'

// Utils
import {clamp} from 'src/shared/utils/vis'

// Types
import {Threshold} from 'src/types'

// Constants
const DRAGGABLE_THRESHOLD_PRECISION = 2

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

    if (
      nextThreshold.type === 'range' &&
      nextThreshold.min > nextThreshold.max
    ) {
      // If the user drags the min past the max or vice versa, we swap the
      // values that are set so that the min is always at most the max
      const maxValue = nextThreshold.min

      nextThreshold.min = nextThreshold.max
      nextThreshold.max = maxValue
    }

    const nextThresholds = thresholds.map((t, i) =>
      i === index ? nextThreshold : t
    )

    const roundedThresholds = nextThresholds.map(nt => {
      if (nt.type === 'greater' || nt.type === 'lesser') {
        return {...nt, value: round(nt.value, DRAGGABLE_THRESHOLD_PRECISION)}
      }

      if (nt.type === 'range') {
        return {
          ...nt,
          min: round(nt.min, DRAGGABLE_THRESHOLD_PRECISION),
          max: round(nt.max, DRAGGABLE_THRESHOLD_PRECISION),
        }
      }
    })

    onSetThresholds(roundedThresholds)
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
          case 'lesser':
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
            throw new Error('Unknown threshold type in <ThresholdMarkers /> ')
        }
      })}
    </div>
  )
}

export default ThresholdMarkers

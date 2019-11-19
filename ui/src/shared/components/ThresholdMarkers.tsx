// Libraries
import React, {useRef, useState, FunctionComponent} from 'react'
import {Scale} from '@influxdata/giraffe'
import {debounce, round} from 'lodash'

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
const ZOOM_IN_WAIT = 500

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

  const nextZoomThreshold: {
    index?: number
    minValue?: number
    maxValue?: number
  } = {}

  const [zoomThreshold, setZoomThreshold] = useState(nextZoomThreshold)

  const zoomIn = () => {
    onSetThresholds(
      thresholds.map((t, i) => {
        if (zoomThreshold.index !== i) {
          return t
        }
        if (t.type === 'greater' || t.type === 'lesser') {
          return {...t, max: t.value}
        }
        if (t.type === 'range') {
          return {
            ...t,
            max: zoomThreshold.maxValue,
            min: zoomThreshold.minValue,
          }
        }
      })
    )
  }

  const debouncedZoomIn = debounce(zoomIn, ZOOM_IN_WAIT)

  const handleDrag = (index: number, field: string, y: number) => {
    nextZoomThreshold.index = index
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
        return {
          ...nt,
          value: round(nt.value, DRAGGABLE_THRESHOLD_PRECISION),
          max: yDomain[1],
        }
      }

      if (nt.type === 'range') {
        if (field === 'maxValue') {
          nextZoomThreshold.maxValue = round(
            nt['maxValue'],
            DRAGGABLE_THRESHOLD_PRECISION
          )
          nextZoomThreshold.minValue = round(
            nt.min,
            DRAGGABLE_THRESHOLD_PRECISION
          )
          return {
            ...nt,
            max: round(yDomain[1], DRAGGABLE_THRESHOLD_PRECISION),
            min: round(nt.min, DRAGGABLE_THRESHOLD_PRECISION),
          }
        }
        nextZoomThreshold.maxValue = round(
          nt.max,
          DRAGGABLE_THRESHOLD_PRECISION
        )
        nextZoomThreshold.minValue = round(
          nt['minValue'],
          DRAGGABLE_THRESHOLD_PRECISION
        )
        return {
          ...nt,
          max: round(nt.max, DRAGGABLE_THRESHOLD_PRECISION),
          min: yDomain[0]
            ? round(yDomain[0], DRAGGABLE_THRESHOLD_PRECISION)
            : 0,
        }
      }
    })
    setZoomThreshold(nextZoomThreshold)
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
                onMouseUp={debouncedZoomIn}
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
                onMouseUp={debouncedZoomIn}
              />
            )
          case 'range':
            return (
              <RangeThresholdMarkers
                key={index}
                yScale={yScale}
                yDomain={yDomain}
                threshold={{
                  ...threshold,
                  min: zoomThreshold.minValue
                    ? zoomThreshold.minValue
                    : round(threshold.min, DRAGGABLE_THRESHOLD_PRECISION),
                  max: zoomThreshold.maxValue
                    ? zoomThreshold.maxValue
                    : round(threshold.max, DRAGGABLE_THRESHOLD_PRECISION),
                }}
                onChangeMinPos={onChangeMinPos}
                onChangeMaxPos={onChangeMaxPos}
                onMouseUp={debouncedZoomIn}
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

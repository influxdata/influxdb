// Libraries
import React, {FC, useState, useRef} from 'react'

// Utils
import {Scale} from '@influxdata/giraffe'
import {isInDomain} from 'src/shared/utils/vis'

// Components
import BoxTooltip from './BoxTooltip'

//Types
import {StatusRow, LowercaseCheckStatusLevel} from 'src/types'
import EventMarkerTooltip from './EventMarkerTooltip'

interface Props {
  events: StatusRow[]
  xScale: Scale<number, number>
  xDomain: number[]
  xFormatter: (x: number) => string
}

const findMaxLevel = (event: StatusRow[]) => {
  const levels: LowercaseCheckStatusLevel[] = [
    'crit',
    'warn',
    'info',
    'ok',
    'unknown',
  ]
  const eventLevels = event.map(e => e.level)
  for (const l of levels) {
    if (eventLevels.includes(l)) {
      return l
    }
  }
  return 'unknown'
}

const EventMarker: FC<Props> = ({xScale, xDomain, events, xFormatter}) => {
  const trigger = useRef<HTMLDivElement>(null)

  const [tooltipVisible, setTooltipVisible] = useState(false)
  let triggerRect: DOMRect = null

  if (trigger.current) {
    triggerRect = trigger.current.getBoundingClientRect()
  }

  const {time} = events[0]
  const x = Math.ceil(xScale(time))
  const style = {left: `${x}px`}

  const level = findMaxLevel(events)
  const markerClass = `event-marker--line__${level.toLowerCase()}`

  const formattedEvents = events.map(e => ({...e, time: xFormatter(e.time)}))

  return (
    isInDomain(time, xDomain) && (
      <>
        <div className={markerClass} style={style}>
          <div
            className="event-marker--line-rect"
            ref={trigger}
            onMouseOver={() => {
              setTooltipVisible(true)
            }}
            onMouseOut={() => {
              setTooltipVisible(false)
            }}
          />
        </div>
        {tooltipVisible && trigger.current && (
          <BoxTooltip triggerRect={triggerRect} maxWidth={500}>
            <EventMarkerTooltip events={formattedEvents} />
          </BoxTooltip>
        )}
      </>
    )
  )
}

export default EventMarker

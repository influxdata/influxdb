// Libraries
import React, {FC, useState, useRef} from 'react'

// Utils
import {Scale} from '@influxdata/giraffe'
import {isInDomain} from 'src/shared/utils/vis'

// Components
import BoxTooltip from './BoxTooltip'

//Types
import {StatusRow, CheckStatusLevel} from 'src/types'

interface Props {
  event: StatusRow[]
  xScale: Scale<number, number>
  xDomain: number[]
}

const findMaxLevel = (event: StatusRow[]): CheckStatusLevel => {
  const levels: CheckStatusLevel[] = ['CRIT', 'WARN', 'INFO', 'OK']
  const eventLevels = event.map(e => e.level)
  for (let l of levels) {
    if (eventLevels.includes(l)) {
      return l
    }
  }
  return 'UNKNOWN'
}

const EventMarker: FC<Props> = ({xScale, xDomain, event}) => {
  const trigger = useRef<HTMLDivElement>(null)

  const [tooltipVisible, setTooltipVisible] = useState(false)
  let triggerRect: DOMRect = null

  if (trigger.current) {
    triggerRect = trigger.current.getBoundingClientRect() as DOMRect
  }

  const {time} = event[0]
  const level = findMaxLevel(event)

  const x = xScale(time)
  const style = {left: `${x}px`}
  const levelClass = `event-marker--${level.toLowerCase()}`

  return (
    <>
      {isInDomain(time, xDomain) && (
        <div
          className={`event-marker--line ${levelClass}`}
          style={style}
          onMouseEnter={() => setTooltipVisible(true)}
          onMouseLeave={() => setTooltipVisible(false)}
        >
          <div className="event-marker--line-triangle" ref={trigger} />
          {tooltipVisible && (
            <BoxTooltip triggerRect={triggerRect as DOMRect}>
              <div> omgomg</div>
            </BoxTooltip>
          )}
        </div>
      )}
    </>
  )
}

export default EventMarker

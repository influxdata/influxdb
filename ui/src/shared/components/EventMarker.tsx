// Libraries
import React, {FC} from 'react'
import {Scale} from '@influxdata/giraffe'
import {isInDomain} from 'src/shared/utils/vis'
import {StatusRow, CheckStatusLevel} from 'src/types'

interface Props {
  event: StatusRow[]
  xScale: Scale<number, number>
  xDomain: number[]
  onHover: (time: number, visible: boolean) => () => void
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

const EventMarker: FC<Props> = ({xScale, xDomain, event, onHover}) => {
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
          onMouseEnter={onHover(time, true)}
          onMouseLeave={onHover(time, false)}
        >
          <div className="event-marker--line-triangle" />
        </div>
      )}
    </>
  )
}

export default EventMarker

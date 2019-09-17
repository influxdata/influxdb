// Libraries
import React, {FC} from 'react'
import {Scale} from '@influxdata/giraffe'
import {isInDomain} from 'src/shared/utils/vis'
import {StatusRow} from 'src/types'

interface Props {
  event: StatusRow
  xScale: Scale<number, number>
  xDomain: number[]
}

const EventMarker: FC<Props> = ({xScale, xDomain, event: {time, level}}) => {
  const x = xScale(time)
  const style = {left: `${x}px`}
  const levelClass = `event-marker--${level.toLowerCase()}`

  return (
    <>
      {isInDomain(time, xDomain) && (
        <div className={`event-marker--line ${levelClass}`} style={style}>
          <div className="event-marker--line-triangle" />
        </div>
      )}
    </>
  )
}

export default EventMarker

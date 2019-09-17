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

const EventMarker: FC<Props> = ({xScale, xDomain, event: {time}}) => {
  const x = xScale(time)
  const style = {left: `${x}px`}

  return (
    <>
      {isInDomain(time, xDomain) && (
        <div className="event-marker--line" style={style} />
      )}
    </>
  )
}

export default EventMarker

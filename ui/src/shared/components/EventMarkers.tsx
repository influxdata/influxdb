// Libraries
import React, {useRef, FunctionComponent} from 'react'

// Components
import EventMarker from 'src/shared/components/EventMarker'

// Utils

// Types
import {Scale} from '@influxdata/giraffe'
import {StatusRow} from 'src/types'

interface Props {
  events: StatusRow[][]
  xScale: Scale<number, number>
  xDomain: number[]
}

const EventMarkers: FunctionComponent<Props> = ({xScale, xDomain, events}) => {
  const originRef = useRef<HTMLDivElement>(null)

  return (
    <div className="event-markers" ref={originRef}>
      {events
        .filter(e => e.length)
        .map((event, index) => {
          return (
            <EventMarker
              key={index}
              xScale={xScale}
              xDomain={xDomain}
              event={event}
              onHover={(time, visible) => () => {
                console.log(time, visible)
              }}
            />
          )
        })}
    </div>
  )
}

export default EventMarkers

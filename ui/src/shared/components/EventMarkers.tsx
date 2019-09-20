// Libraries
import React, {FunctionComponent} from 'react'

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
  return (
    <div className="event-markers">
      {events
        .filter(e => e.length)
        .map((event, index) => {
          return (
            <EventMarker
              key={index}
              xScale={xScale}
              xDomain={xDomain}
              event={event}
            />
          )
        })}
    </div>
  )
}

export default EventMarkers

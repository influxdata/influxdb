// Libraries
import React, {FunctionComponent} from 'react'

// Components
import EventMarker from 'src/shared/components/EventMarker'

// Utils

// Types
import {Scale} from '@influxdata/giraffe'
import {StatusRow} from 'src/types'

interface Props {
  eventsArray: StatusRow[][]
  xScale: Scale<number, number>
  xDomain: number[]
}

const EventMarkers: FunctionComponent<Props> = ({
  xScale,
  xDomain,
  eventsArray,
}) => {
  return (
    <div className="event-markers">
      {eventsArray
        .filter(events => events.length)
        .map((events, index) => {
          return (
            <EventMarker
              key={index}
              xScale={xScale}
              xDomain={xDomain}
              events={events}
            />
          )
        })}
    </div>
  )
}

export default EventMarkers

// Libraries
import React, {FunctionComponent} from 'react'

// Components
import EventMarker from 'src/shared/components/EventMarker'

// Types
import {Scale} from '@influxdata/giraffe'
import {StatusRow} from 'src/types'

interface Props {
  eventsArray: StatusRow[][]
  xScale: Scale<number, number>
  xDomain: number[]
  xFormatter: (x: number) => string
}

const EventMarkers: FunctionComponent<Props> = ({
  xScale,
  xDomain,
  eventsArray,
  xFormatter,
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
              xFormatter={xFormatter}
            />
          )
        })}
    </div>
  )
}

export default EventMarkers

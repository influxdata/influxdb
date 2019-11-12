// Libraries
import React, {FunctionComponent, useState, useEffect} from 'react'

// Components
import EventMarker from 'src/shared/components/EventMarker'

// Types
import {Scale} from '@influxdata/giraffe'
import {StatusRow} from 'src/types'
import {IconFont, Icon} from '@influxdata/clockface'
import {LEVEL_COLORS} from 'src/alerting/constants'

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
  const [isOkVisible, setOkVisibility] = useState(false)
  const [isInfoVisible, setInfoVisibility] = useState(true)
  const [isWarnVisible, setWarnVisibility] = useState(true)
  const [isCritVisible, setCritVisibility] = useState(true)
  const [isUnknownVisible, setUnknownVisibility] = useState(true)

  const [filteredEventsArray, setFilteredEventsArray] = useState(eventsArray)

  useEffect(() => {
    setFilteredEventsArray(
      eventsArray
        .map(events => {
          return (
            events.length &&
            events.filter(e => {
              return (
                (e.level === 'ok' && isOkVisible) ||
                (e.level === 'info' && isInfoVisible) ||
                (e.level === 'warn' && isWarnVisible) ||
                (e.level === 'crit' && isCritVisible) ||
                (e.level === 'unknown' && isUnknownVisible)
              )
            })
          )
        })
        .filter(events => events && events.length)
    )
  }, [
    eventsArray,
    isOkVisible,
    isInfoVisible,
    isWarnVisible,
    isCritVisible,
    isUnknownVisible,
  ])

  const iconGlyph = (visible: boolean) => {
    return visible ? IconFont.Eye : IconFont.EyeClosed
  }

  const eventVisibilityIndicators = (
    <div className="event-marker--vis-selector">
      <span onClick={() => setOkVisibility(!isOkVisible)}>
        <Icon
          className="event-marker--vis-icon"
          style={{color: LEVEL_COLORS['OK']}}
          glyph={iconGlyph(isOkVisible)}
        />
      </span>
      <span onClick={() => setWarnVisibility(!isWarnVisible)}>
        <Icon
          className="event-marker--vis-icon"
          style={{color: LEVEL_COLORS['WARN']}}
          glyph={iconGlyph(isWarnVisible)}
        />
      </span>
      <span onClick={() => setInfoVisibility(!isInfoVisible)}>
        <Icon
          className="event-marker--vis-icon"
          style={{color: LEVEL_COLORS['INFO']}}
          glyph={iconGlyph(isInfoVisible)}
        />
      </span>
      <span onClick={() => setCritVisibility(!isCritVisible)}>
        <Icon
          className="event-marker--vis-icon"
          style={{color: LEVEL_COLORS['CRIT']}}
          glyph={iconGlyph(isCritVisible)}
        />
      </span>
      <span onClick={() => setUnknownVisibility(!isUnknownVisible)}>
        <Icon
          className="event-marker--vis-icon"
          style={{color: LEVEL_COLORS['UNKNOWN']}}
          glyph={iconGlyph(isUnknownVisible)}
        />
      </span>
    </div>
  )

  return (
    <>
      <div className="event-markers">
        {filteredEventsArray.map((events, index) => {
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
      {eventVisibilityIndicators}
    </>
  )
}

export default EventMarkers

// Libraries
import React, {FunctionComponent, Fragment, CSSProperties} from 'react'

// Constants
import {LEVEL_COLORS} from 'src/alerting/constants'

interface EventsRow {
  time: string
  checkName: string
  level: string
  message: string
}

interface Props {
  events: EventsRow[]
}

const EventMarkerTooltip: FunctionComponent<Props> = ({events}) => {
  const columns = ['time', 'checkName', 'level', 'message']

  const calculateLevelStyle = (
    level: string,
    colorize: boolean
  ): CSSProperties => {
    if (!colorize) {
      return
    }

    const color = LEVEL_COLORS[`${level.toUpperCase()}`]
    return {color}
  }

  return (
    <div className="box-tooltip--contents event-marker-tooltip">
      <div className="event-marker-tooltip--table">
        {columns.map(colName => (
          <div key={colName} className="event-marker-tooltip--column">
            <div className="event-marker-tooltip--header">{colName}</div>
            {events.map((event, i) => {
              const {time, checkName, level} = event
              return (
                <Fragment key={`${time}-${checkName}-${level}-${i}`}>
                  <div
                    className="event-marker-tooltip--cell"
                    style={calculateLevelStyle(level, colName === 'level')}
                  >
                    {event[colName]}
                  </div>
                </Fragment>
              )
            })}
          </div>
        ))}
      </div>
    </div>
  )
}

export default EventMarkerTooltip

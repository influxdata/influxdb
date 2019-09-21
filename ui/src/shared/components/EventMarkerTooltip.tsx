// Libraries
import React, {FunctionComponent} from 'react'

// Components

// Types
import {StatusRow} from 'src/types'

interface Props {
  events: StatusRow[]
}

const columns = ['time', 'checkName', 'level', 'message']

const EventMarkerTooltip: FunctionComponent<Props> = ({events}) => {
  // return <div className="box-tooltip--contents" />
  return (
    <div
      className="giraffe-tooltip"
      style={{
        border: 'pink',
        backgroundColor: 'gray',
        color: 'green',
        borderRadius: '3px',
        padding: '10px',
        cursor: 'crosshair',
      }}
      data-testid="giraffe-tooltip"
    >
      <div
        className="giraffe-tooltip-table"
        style={{
          display: 'flex',
          justifyContent: 'space-between',
        }}
        data-testid="giraffe-tooltip-table"
      >
        {columns.map(c => (
          <div
            key={c}
            className="giraffe-tooltip-column"
            style={{
              marginRight: '15px',
              textAlign: 'right',
            }}
          >
            <div
              className="giraffe-tooltip-column-header"
              style={{marginBottom: '5px', color: 'blue'}}
            >
              {c}
            </div>
            {events.map((event, j) => (
              <div
                className="giraffe-tooltip-column-value"
                key={j}
                style={{
                  color: 'black',
                  maxWidth: '200px',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {event[c]}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

export default EventMarkerTooltip

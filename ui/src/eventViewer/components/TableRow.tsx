import React, {FC, CSSProperties} from 'react'

import {Row, Fields} from 'src/eventViewer/types'

interface Props {
  row: Row
  style: CSSProperties
  fields: Fields
}

const TableRow: FC<Props> = ({row, style, fields}) => {
  return (
    <div style={style}>
      <div className="event-row">
        {fields.map(({component: Component, columnWidth, rowKey}) => {
          const style = {flexBasis: `${columnWidth}px`}

          let content

          if (row[rowKey] === undefined) {
            content = null
          } else if (Component) {
            content = <Component key={rowKey} row={row} />
          } else {
            content = String(row[rowKey])
          }

          return (
            <div key={rowKey} className="event-row--field" style={style}>
              {content}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default TableRow

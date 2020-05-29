// Libraries
import React, {FC} from 'react'

// Types
import {Fields} from 'src/eventViewer/types'

interface Props {
  fields: Fields
}

const Header: FC<Props> = ({fields}) => {
  return (
    <div className="event-table-header">
      {fields.map(({rowKey, columnWidth, columnName}) => {
        const style = {flexBasis: `${columnWidth}px`}

        return (
          <div key={rowKey} className="event-table-header--field" style={style}>
            {columnName}
          </div>
        )
      })}
    </div>
  )
}

export default Header

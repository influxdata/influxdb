import React, {FC, CSSProperties} from 'react'

import {Row, FieldComponents} from 'src/eventViewer/types'

interface Props {
  row: Row
  style: CSSProperties
  fields: string[]
  fieldWidths: {[field: string]: number}
  fieldComponents: FieldComponents
}

const TableRow: FC<Props> = ({
  row,
  style,
  fields,
  fieldComponents,
  fieldWidths,
}) => {
  return (
    <div style={style}>
      <div className="event-row">
        {fields.map(field => {
          const Component = fieldComponents[field]
          const style = {flexBasis: `${fieldWidths[field]}px`}

          let content

          if (row[field] === undefined) {
            content = null
          } else if (Component) {
            content = <Component key={field} row={row} />
          } else {
            content = String(row[field])
          }

          return (
            <div key={field} className="event-row--field" style={style}>
              {content}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default TableRow

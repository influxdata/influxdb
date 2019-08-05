import React, {FC} from 'react'

// Adds spaces to camelcased names, e.g. "checkName" to "check Name"
const stylizeName = (name: string): string => {
  let result = ''

  for (const c of name) {
    if (c === c.toUpperCase()) {
      result = result + ' ' + c
    } else {
      result = result + c
    }
  }

  return result.toUpperCase()
}

interface Props {
  fields: string[]
  fieldWidths: {[field: string]: number}
}

const Header: FC<Props> = ({fields, fieldWidths}) => {
  return (
    <div className="event-table-header">
      {fields.map(field => {
        const style = {flexBasis: `${fieldWidths[field]}px`}

        return (
          <div key={field} className="event-table-header--field" style={style}>
            {stylizeName(field)}
          </div>
        )
      })}
    </div>
  )
}

export default Header

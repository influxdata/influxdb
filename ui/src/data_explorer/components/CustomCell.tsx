import React, {SFC} from 'react'

import moment from 'moment'

export interface CustomCellProps {
  data?: string
  columnName?: string
}

const CustomCell: SFC<CustomCellProps> = ({data, columnName}) => {
  if (columnName === 'time') {
    const date = moment(new Date(data)).format('MM/DD/YY hh:mm:ssA')

    return (
      <span>
        {date}
      </span>
    )
  }

  return (
    <span>
      {data}
    </span>
  )
}

export default CustomCell

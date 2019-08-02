// Libraries
import React, {FC} from 'react'

// Components
import {Button, IconFont} from '@influxdata/clockface'

const style = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
}

interface Props {
  title: string
  onCreate: () => void
}

const AlertsColumnHeader: FC<Props> = ({onCreate, title}) => {
  return (
    <div style={style}>
      {title}
      <Button
        text="Create"
        icon={IconFont.AddCell}
        onClick={onCreate}
        testID="open-create-rule--button"
      />
    </div>
  )
}

export default AlertsColumnHeader

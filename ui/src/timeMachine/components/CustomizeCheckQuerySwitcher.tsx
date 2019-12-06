// Libraries
import React, {FC} from 'react'

// Components
import {Button} from '@influxdata/clockface'

const CustomizeCheckQuerySwitcher: FC = () => {
  return (
    <Button
      text="Customize Check Query"
      titleText="Switch to Script Editor"
      onClick={() => console.log('will do something soon')}
      testID="switch-to-custom-check"
    />
  )
}

export default CustomizeCheckQuerySwitcher

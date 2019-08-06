// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Button} from '@influxdata/clockface'

const HelpButton: FunctionComponent = () => {
  const handleClick = () => {}

  return (
    <Button
      titleText="Learn more about alerting"
      text="Help"
      onClick={handleClick}
    />
  )
}

export default HelpButton

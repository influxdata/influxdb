// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Button} from '@influxdata/clockface'

const HelpButton: FunctionComponent = () => {
  const handleClick = () => {
    const newTab = window.open(
      'https://github.com/influxdata/docs-v2/blob/monitor-alert/content/v2.0/monitor-alert/checks/create.md#configure-the-check'
    )
    newTab.focus()
  }

  return (
    <Button
      titleText="Learn more about alerting"
      text="Help"
      onClick={handleClick}
    />
  )
}

export default HelpButton

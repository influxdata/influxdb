// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Actions
import {loadCustomQueryState} from 'src/timeMachine/actions'

interface DispatchProps {
  onLoadCustomQueryState: typeof loadCustomQueryState
}

const CustomizeCheckQueryButton: FC<DispatchProps> = ({
  onLoadCustomQueryState,
}) => {
  const switchToEditor = () => {
    onLoadCustomQueryState()
  }
  return (
    <Button
      text="Customize Check Query"
      titleText="Switch to Script Editor"
      onClick={switchToEditor}
      testID="switch-to-custom-check"
    />
  )
}

const mdtp: DispatchProps = {
  onLoadCustomQueryState: loadCustomQueryState,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(CustomizeCheckQueryButton)

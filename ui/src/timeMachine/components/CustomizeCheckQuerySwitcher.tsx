// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

interface DispatchProps {
  onSetActiveTab: typeof setActiveTab
}

const CustomizeCheckQuerySwitcher: FC<DispatchProps> = ({onSetActiveTab}) => {
  return (
    <Button
      text="Customize Check Query"
      titleText="Switch to Script Editor"
      onClick={() => onSetActiveTab('customCheckQuery')}
      testID="switch-to-custom-check"
    />
  )
}

const mdtp: DispatchProps = {
  onSetActiveTab: setActiveTab,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(CustomizeCheckQuerySwitcher)

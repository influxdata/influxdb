// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Actions
import {setType as setViewType} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'
import {setActiveTab} from 'src/timeMachine/actions'

//Types
import {RemoteDataState} from 'src/types'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
  setViewType: typeof setViewType
  setCurrentCheck: typeof setCurrentCheck
}

type Props = DispatchProps

const RemoveButton: FunctionComponent<Props> = ({
  setActiveTab,
  setViewType,
  setCurrentCheck,
}) => {
  const handleClick = () => {
    setCurrentCheck(RemoteDataState.NotStarted, null)
    setViewType('xy')
    setActiveTab('queries')
  }

  return (
    <Button
      titleText="Remove Check from Cell"
      text="Remove Check from Cell"
      onClick={handleClick}
    />
  )
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
  setViewType: setViewType,
  setCurrentCheck: setCurrentCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(RemoveButton)

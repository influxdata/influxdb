// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Actions
import {removeCheck} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'

//Types
import {RemoteDataState} from 'src/types'

interface DispatchProps {
  removeCheck: typeof removeCheck
  setCurrentCheck: typeof setCurrentCheck
}

type Props = DispatchProps

const RemoveButton: FunctionComponent<Props> = ({
  setCurrentCheck,
  removeCheck,
}) => {
  const handleClick = () => {
    setCurrentCheck(RemoteDataState.NotStarted, null)
    removeCheck()
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
  removeCheck: removeCheck,
  setCurrentCheck: setCurrentCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(RemoveButton)

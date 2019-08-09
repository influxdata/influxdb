// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Actions
import {removeCheckFromView} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'

//Types
import {RemoteDataState} from 'src/types'

interface DispatchProps {
  removeCheckFromView: typeof removeCheckFromView
  setCurrentCheck: typeof setCurrentCheck
}

type Props = DispatchProps

const RemoveButton: FunctionComponent<Props> = ({
  removeCheckFromView,
  setCurrentCheck,
}) => {
  const handleClick = () => {
    removeCheckFromView()
    setCurrentCheck(RemoteDataState.NotStarted, null)
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
  removeCheckFromView: removeCheckFromView,
  setCurrentCheck: setCurrentCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(RemoveButton)

// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button} from '@influxdata/clockface'

// Actions
import {convertFromCheckView} from 'src/timeMachine/actions'
import {setCurrentCheck} from 'src/alerting/actions/checks'

//Types
import {RemoteDataState} from 'src/types'

interface DispatchProps {
  onConvertFromCheckView: typeof convertFromCheckView
  onSetCurrentCheck: typeof setCurrentCheck
}

type Props = DispatchProps

const RemoveButton: FunctionComponent<Props> = ({
  onSetCurrentCheck,
  onConvertFromCheckView,
}) => {
  const handleClick = () => {
    onConvertFromCheckView()

    // TODO: Move the current check state into the time machine reducer, then
    // handle this state transition as part `CONVERT_FROM_CHECK_VIEW`
    // transition
    onSetCurrentCheck(RemoteDataState.NotStarted, null)
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
  onConvertFromCheckView: convertFromCheckView,
  onSetCurrentCheck: setCurrentCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(RemoveButton)

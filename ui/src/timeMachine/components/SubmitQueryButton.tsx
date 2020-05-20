// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types'

interface StateProps {
  submitButtonDisabled: boolean
  queryStatus: RemoteDataState
}

interface DispatchProps {
  onSubmit: typeof saveAndExecuteQueries | (() => void)
}

type Props = StateProps & DispatchProps

class SubmitQueryButton extends PureComponent<Props> {
  public render() {
    return (
      <Button
        text="Submit"
        size={ComponentSize.Small}
        status={this.buttonStatus}
        onClick={this.handleClick}
        color={ComponentColor.Primary}
        testID="time-machine-submit-button"
      />
    )
  }

  private get buttonStatus(): ComponentStatus {
    const {queryStatus, submitButtonDisabled} = this.props

    if (submitButtonDisabled) {
      return ComponentStatus.Disabled
    }

    if (queryStatus === RemoteDataState.Loading) {
      return ComponentStatus.Loading
    }

    return ComponentStatus.Default
  }

  private handleClick = (): void => {
    this.props.onSubmit()
  }
}

export {SubmitQueryButton}

const mstp = (state: AppState) => {
  const submitButtonDisabled = getActiveQuery(state).text === ''
  const queryStatus = getActiveTimeMachine(state).queryResults.status

  return {submitButtonDisabled, queryStatus}
}

const mdtp = {
  onSubmit: saveAndExecuteQueries,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(SubmitQueryButton)

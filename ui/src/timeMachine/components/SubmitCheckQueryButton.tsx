// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {executeCheckQuery} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

interface State {
  didClick: boolean
}

class SubmitQueryButton extends PureComponent<Props, State> {
  public state: State = {didClick: false}

  public componentDidUpdate(prevProps: Props) {
    if (
      prevProps.queryStatus === RemoteDataState.Loading &&
      this.props.queryStatus === RemoteDataState.Done
    ) {
      this.setState({didClick: false})
    }
  }

  public render() {
    return (
      <Button
        text="Execute check query and view statuses"
        size={ComponentSize.Small}
        status={this.buttonStatus}
        onClick={this.handleClick}
        color={ComponentColor.Primary}
        testID="time-machine-check-query-run-button"
      />
    )
  }

  private get buttonStatus(): ComponentStatus {
    const {queryStatus, submitButtonDisabled} = this.props
    const {didClick} = this.state

    if (submitButtonDisabled) {
      return ComponentStatus.Disabled
    }

    if (queryStatus === RemoteDataState.Loading && didClick) {
      // Only show loading state for button if it was just clicked
      return ComponentStatus.Loading
    }

    return ComponentStatus.Default
  }

  private handleClick = (): void => {
    this.props.onExecuteCheckQuery()
    this.setState({didClick: true})
  }
}

const mstp = (state: AppState) => {
  const submitButtonDisabled = getActiveQuery(state).text === ''
  const queryStatus = getActiveTimeMachine(state).queryResults.status

  return {submitButtonDisabled, queryStatus}
}

const mdtp = {
  onExecuteCheckQuery: executeCheckQuery,
}

const connector = connect(mstp, mdtp)

export default connector(SubmitQueryButton)

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
import {submitQueriesWithVars} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'

interface StateProps {
  submitButtonDisabled: boolean
}

interface DispatchProps {
  onSubmitQueries: typeof submitQueriesWithVars
}

interface OwnProps {
  queryStatus: RemoteDataState
}

type Props = StateProps & DispatchProps & OwnProps

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
    this.props.onSubmitQueries()
    this.setState({didClick: true})
  }
}

const mstp = (state: AppState) => {
  const submitButtonDisabled = getActiveQuery(state).text === ''

  return {submitButtonDisabled}
}

const mdtp = {
  onSubmitQueries: submitQueriesWithVars,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(SubmitQueryButton)

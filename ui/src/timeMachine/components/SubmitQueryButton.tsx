// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  IconFont,
} from '@influxdata/clockface'

// Actions
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'
import {reportSimpleQueryPerformanceEvent} from 'src/cloud/utils/reporting'

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

interface OwnProps {
  text?: string
  icon?: IconFont
  testID?: string
}

type Props = OwnProps & StateProps & DispatchProps

class SubmitQueryButton extends PureComponent<Props> {
  public static defaultProps = {
    text: 'Submit',
    testID: 'time-machine-submit-button',
  }

  public render() {
    const {text, icon, testID} = this.props

    return (
      <Button
        text={text}
        icon={icon}
        size={ComponentSize.Small}
        status={this.buttonStatus}
        onClick={this.handleClick}
        color={ComponentColor.Primary}
        testID={testID}
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
    reportSimpleQueryPerformanceEvent('SubmitQueryButton click')
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

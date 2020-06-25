// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

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
import {notify} from 'src/shared/actions/notifications'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'
import {reportSimpleQueryPerformanceEvent} from 'src/cloud/utils/reporting'
import {queryCancelRequest} from 'src/shared/copy/notifications'

// Types
import {AppState, RemoteDataState} from 'src/types'

interface OwnProps {
  text?: string
  icon?: IconFont
  testID?: string
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const DELAYTIME = 2000


class SubmitQueryButton extends PureComponent<Props> {
  public static defaultProps = {
    text: 'Submit',
    testID: 'time-machine-submit-button',
  }

  public state = {
    timer: false
  }

  private timer

  public componentDidUpdate(prevProps) {
    if (this.props.queryStatus !== prevProps.queryStatus && prevProps.queryStatus === RemoteDataState.Loading) {
      if(this.timer){
        clearTimeout(this.timer)
        delete this.timer
      }

      this.setState({timer :false})
    }
  }

  public render() {
    const {text, queryStatus, icon, testID} = this.props

    if (queryStatus === RemoteDataState.Loading && this.state.timer === true) {
      return (
        <Button
        text="Cancel"
        icon={icon}
        size={ComponentSize.Small}
        status={ComponentStatus.Default}
        onClick={this.handleCancelClick}
        color={ComponentColor.Danger}
        testID={testID}
        style={{width:'100px'}}
      />
      )
    }
    return (
      <Button
        text={text}
        icon={icon}
        size={ComponentSize.Small}
        status={this.buttonStatus}
        onClick={this.handleClick}
        color={ComponentColor.Primary}
        testID={testID}
        style={{width:'100px'}}
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

  private abortController: AbortController

  private handleClick = (): void => {
    // Optimistic UI, set the state to loading when the event
    // happens rather than when the event trickles down to execution
    reportSimpleQueryPerformanceEvent('SubmitQueryButton click')
    // We need to instantiate a new AbortController per request
    // In order to allow for requests after cancellations:
    // https://stackoverflow.com/a/56548348/7963795

    this.timer = setTimeout(() =>{
      this.setState({timer: true})
    }, DELAYTIME);
    this.abortController = new AbortController()
    this.props.onSubmit(this.abortController)
  }

  private handleCancelClick = (): void => {
    if (this.props.onNotify) {
      this.props.onNotify(queryCancelRequest())
    }
    if (this.abortController) {
      this.abortController.abort()
      this.abortController = null
    }
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
  onNotify: notify,
}

const connector = connect(mstp, mdtp)

export default connector(SubmitQueryButton)

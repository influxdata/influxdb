// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import moment from 'moment'

// Components
import {Overlay, IndexList} from '@influxdata/clockface'
import RunLogsOverlay from 'src/tasks/components/RunLogsList'

// Actions
import {getLogs} from 'src/tasks/actions/thunks'

// Types
import {ComponentSize, ComponentColor, Button} from '@influxdata/clockface'
import {AppState, LogEvent, Run} from 'src/types'
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'

interface OwnProps {
  taskID: string
  run: Run
}

interface DispatchProps {
  getLogs: typeof getLogs
}

interface StateProps {
  logs: LogEvent[]
}

type Props = OwnProps & DispatchProps & StateProps

interface State {
  isImportOverlayVisible: boolean
}

class TaskRunsRow extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isImportOverlayVisible: false,
    }
  }

  public render() {
    const {run} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>{run.status}</IndexList.Cell>
          <IndexList.Cell>
            {this.dateTimeString(run.scheduledFor)}
          </IndexList.Cell>
          <IndexList.Cell>{this.dateTimeString(run.startedAt)}</IndexList.Cell>
          <IndexList.Cell>{run.duration}</IndexList.Cell>
          <IndexList.Cell>
            <Button
              key={run.id}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              text="View Logs"
              onClick={this.handleToggleOverlay}
            />
            {this.renderLogOverlay}
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private dateTimeString(dt: string): string {
    if (!dt) {
      return ''
    }
    const newdate = new Date(dt)
    const formatted = moment(newdate).format(DEFAULT_TIME_FORMAT)

    return formatted
  }

  private handleToggleOverlay = () => {
    const {taskID, run, getLogs} = this.props
    getLogs(taskID, run.id)

    this.setState({isImportOverlayVisible: !this.state.isImportOverlayVisible})
  }

  private get renderLogOverlay(): JSX.Element {
    const {isImportOverlayVisible} = this.state
    const {logs} = this.props

    return (
      <Overlay visible={isImportOverlayVisible}>
        <RunLogsOverlay
          onDismissOverlay={this.handleToggleOverlay}
          logs={logs}
        />
      </Overlay>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {logs} = state.resources.tasks

  return {logs}
}

const mdtp: DispatchProps = {getLogs: getLogs}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TaskRunsRow)

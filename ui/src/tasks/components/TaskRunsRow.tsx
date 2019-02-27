// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {IndexList, OverlayTechnology} from 'src/clockface'
import RunLogsOverlay from 'src/tasks/components/RunLogsList'

// Actions
import {getLogs} from 'src/tasks/actions/v2'

// Types
import {ComponentSize, ComponentColor, Button} from '@influxdata/clockface'
import {Run, LogEvent} from '@influxdata/influx'
import {AppState} from 'src/types/v2'

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
          <IndexList.Cell>
            {this.dateTimeString(run.requestedAt)}
          </IndexList.Cell>
          <IndexList.Cell>{this.dateTimeString(run.startedAt)}</IndexList.Cell>
          <IndexList.Cell>{this.dateTimeString(run.finishedAt)}</IndexList.Cell>
          <IndexList.Cell>{run.status}</IndexList.Cell>
          <IndexList.Cell>
            {this.dateTimeString(run.scheduledFor)}
          </IndexList.Cell>
          <IndexList.Cell>
            <Button
              key={run.id}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              text="View Logs"
              onClick={this.handleToggleOverlay}
            />
          </IndexList.Cell>
        </IndexList.Row>

        {this.renderLogOverlay}
      </>
    )
  }

  private dateTimeString(dt: Date): string {
    if (!dt) {
      return ''
    }
    const newdate = new Date(dt)
    const date = newdate.toDateString()
    const time = newdate.toLocaleTimeString()
    const formatted = `${date} ${time}`

    return formatted
  }

  private handleToggleOverlay = async () => {
    const {taskID, run, getLogs} = this.props
    await getLogs(taskID, run.id)

    this.setState({isImportOverlayVisible: !this.state.isImportOverlayVisible})
  }

  private get renderLogOverlay(): JSX.Element {
    const {isImportOverlayVisible} = this.state
    const {logs} = this.props

    return (
      <OverlayTechnology visible={isImportOverlayVisible}>
        <RunLogsOverlay
          onDismissOverlay={this.handleToggleOverlay}
          logs={logs}
        />
      </OverlayTechnology>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    tasks: {logs},
  } = state
  return {logs}
}

const mdtp: DispatchProps = {getLogs: getLogs}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TaskRunsRow)

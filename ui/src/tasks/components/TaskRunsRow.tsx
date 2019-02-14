// Libraries
import React, {PureComponent} from 'react'

// Components
import {Run} from '@influxdata/influx'
import {IndexList, OverlayTechnology} from 'src/clockface'
import {Button} from '@influxdata/clockface'
import RunLogsOverlay from './RunLogsOverlay'

// Types
import {ComponentSize, ComponentColor} from '@influxdata/clockface'

interface Props {
  taskID: string
  run: Run
}

interface State {
  isImportOverlayVisible: boolean
}

export default class TaskRunsRow extends PureComponent<Props, State> {
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
    const date = dt.toDateString()
    const time = dt.toLocaleTimeString()
    const formatted = `${date} ${time}`

    return formatted
  }

  private handleToggleOverlay = () => {
    this.setState({isImportOverlayVisible: !this.state.isImportOverlayVisible})
  }

  private get renderLogOverlay(): JSX.Element {
    const {isImportOverlayVisible} = this.state
    const {taskID, run} = this.props

    return (
      <OverlayTechnology visible={isImportOverlayVisible}>
        <RunLogsOverlay
          onDismissOverlay={this.handleToggleOverlay}
          taskID={taskID}
          runID={run.id}
        />
      </OverlayTechnology>
    )
  }
}

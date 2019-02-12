import React, {PureComponent} from 'react'
import _ from 'lodash'

import Container from 'src/clockface/components/overlays/OverlayContainer'
import Heading from 'src/clockface/components/overlays/OverlayHeading'
import Body from 'src/clockface/components/overlays/OverlayBody'

// DummyData
import {runLogs} from 'src/tasks/dummyData'
import {IndexList} from 'src/clockface'

interface Props {
  onDismissOverlay: () => void
  taskID: string
  runID: string
}

class RunLogsOverlay extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {onDismissOverlay} = this.props

    return (
      <Container maxWidth={800}>
        <Heading title="Run Logs" onDismiss={onDismissOverlay} />
        <Body>
          <IndexList>
            <IndexList.Header>
              <IndexList.HeaderCell columnName="Time" width="50%" />
              <IndexList.HeaderCell columnName="Message" width="50%" />
            </IndexList.Header>
            <IndexList.Body emptyState={<></>} columnCount={2}>
              {this.listLogs}
            </IndexList.Body>
          </IndexList>
        </Body>
      </Container>
    )
  }

  public get listLogs(): JSX.Element[] {
    const logs = runLogs.events.map(rl => (
      <IndexList.Row key={rl.message}>
        <IndexList.Cell>{this.dateTimeString(rl.time)}</IndexList.Cell>
        <IndexList.Cell>{rl.message}</IndexList.Cell>
      </IndexList.Row>
    ))

    return logs
  }

  private dateTimeString(dt: Date): string {
    const date = dt.toDateString()
    const time = dt.toLocaleTimeString()
    const formatted = `${date} ${time}`

    return formatted
  }
}

export default RunLogsOverlay

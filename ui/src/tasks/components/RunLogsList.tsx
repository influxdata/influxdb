// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

//Components
import Container from 'src/clockface/components/overlays/OverlayContainer'
import Heading from 'src/clockface/components/overlays/OverlayHeading'
import Body from 'src/clockface/components/overlays/OverlayBody'
import RunLogRow from 'src/tasks/components/RunLogRow'
import {IndexList} from 'src/clockface'

// Types
import {LogEvent} from '@influxdata/influx'

interface Props {
  onDismissOverlay: () => void
  logs: LogEvent[]
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
    const logs = this.props.logs.map(rl => <RunLogRow log={rl} />)

    return logs
  }
}

export default RunLogsOverlay

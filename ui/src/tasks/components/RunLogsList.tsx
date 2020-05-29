// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

//Components
import {Overlay, IndexList, DapperScrollbars} from '@influxdata/clockface'
import RunLogRow from 'src/tasks/components/RunLogRow'

// Types
import {LogEvent} from 'src/types'

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
      <Overlay.Container className="run-logs--list">
        <Overlay.Header title="Run Logs" onDismiss={onDismissOverlay} />
        <Overlay.Body>
          <DapperScrollbars autoSizeHeight={true} style={{maxHeight: '700px'}}>
            <IndexList>
              <IndexList.Header>
                <IndexList.HeaderCell columnName="Time" width="10%" />
                <IndexList.HeaderCell columnName="Message" width="90%" />
              </IndexList.Header>
              <IndexList.Body emptyState={<></>} columnCount={2}>
                {this.listLogs}
              </IndexList.Body>
            </IndexList>
          </DapperScrollbars>
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  public get listLogs(): JSX.Element[] {
    const logs = this.props.logs.map(rl => (
      <RunLogRow key={rl.message} log={rl} />
    ))

    return logs
  }
}

export default RunLogsOverlay

import React, {PureComponent} from 'react'
import LogsSearchBar from 'src/logs/components/LogsSearchBar'

interface Props {
  thing: string
}

class LogsTableContainer extends PureComponent<Props> {
  public render() {
    return (
      <>
        <LogsSearchBar thing="thing" />
        <div className="logs-viewer--table-container">
          <p>{this.props.thing}</p>
        </div>
      </>
    )
  }
}

export default LogsTableContainer

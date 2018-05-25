import React, {PureComponent} from 'react'

class LogsGraphContainer extends PureComponent {
  public render() {
    return (
      <div className="logs-viewer--graph-container">
        <div className="logs-viewer--graph">{this.props.children}</div>
      </div>
    )
  }
}

export default LogsGraphContainer

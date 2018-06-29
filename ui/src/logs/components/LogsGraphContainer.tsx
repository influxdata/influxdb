import React, {PureComponent} from 'react'

class LogsGraphContainer extends PureComponent {
  public render() {
    return (
      <div className="logs-viewer--graph-container">{this.props.children}</div>
    )
  }
}

export default LogsGraphContainer

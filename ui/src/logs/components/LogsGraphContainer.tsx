import React, {PureComponent} from 'react'

interface Props {
  thing: string
}

class LogsGraphContainer extends PureComponent<Props> {
  public render() {
    return (
      <div className="logs-viewer--graph-container">
        <p>{this.props.thing}</p>
      </div>
    )
  }
}

export default LogsGraphContainer

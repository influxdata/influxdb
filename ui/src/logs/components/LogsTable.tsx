import React, {PureComponent} from 'react'

interface Props {
  thing: string
}

class LogsTableContainer extends PureComponent<Props> {
  public render() {
    return (
      <div className="logs-viewer--table-container">
        <p>{this.props.thing}</p>
      </div>
    )
  }
}

export default LogsTableContainer

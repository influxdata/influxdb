import React, {PureComponent} from 'react'

class InvalidData extends PureComponent<{}> {
  public render() {
    return (
      <div className="graph-empty">
        <p>
          The data returned from the query can't be visualized with this graph
          type.<br />Try updating the query or selecting a different graph type.
        </p>
      </div>
    )
  }
}

export default InvalidData

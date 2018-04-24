import React, {PureComponent} from 'react'

class InvalidData extends PureComponent<{}> {
  public render() {
    return (
      <p
        className="data-error"
        style={{textAlign: 'center', paddingTop: '10px'}}
      >
        The data returned from the query can't be visualized with this graph
        type. Try updating the query or selecting a different graph type.
      </p>
    )
  }
}

export default InvalidData

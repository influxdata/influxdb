// Libraries
import React, {PureComponent, CSSProperties} from 'react'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  loading: RemoteDataState
  children: JSX.Element
}

const GraphLoadingDots = () => (
  <div className="graph-panel__refreshing">
    <div />
    <div />
    <div />
  </div>
)

class DygraphCell extends PureComponent<Props> {
  public render() {
    const {loading} = this.props
    return (
      <div className="dygraph graph--hasYLabel" style={this.style}>
        {loading === RemoteDataState.Loading && <GraphLoadingDots />}
        {this.props.children}
      </div>
    )
  }

  private get style(): CSSProperties {
    return {height: '100%'}
  }
}

export default DygraphCell

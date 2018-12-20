// Libraries
import React, {Component} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
}

class GridRow extends Component<Props> {
  public render() {
    const {children} = this.props

    return <div className="grid--row">{children}</div>
  }
}

export default GridRow

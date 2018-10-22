// Libraries
import React, {Component} from 'react'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
}

@ErrorHandling
class IndexListHeader extends Component<Props> {
  public render() {
    const {children} = this.props

    return (
      <thead className="index-list--header">
        <tr>{children}</tr>
      </thead>
    )
  }
}

export default IndexListHeader

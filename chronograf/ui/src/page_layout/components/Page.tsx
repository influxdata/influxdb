// Libraries
import React, {Component} from 'react'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element | JSX.Element[]
}

@ErrorHandling
class Page extends Component<Props> {
  public render() {
    const {children} = this.props

    return <div className="page">{children}</div>
  }
}

export default Page

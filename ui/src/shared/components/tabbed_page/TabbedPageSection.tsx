// Libraries
import React, {Component} from 'react'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  title: string
  url: string
}

@ErrorHandling
class TabbedPageSection extends Component<Props> {
  public render() {
    return <div>{this.props.children}</div>
  }
}

export default TabbedPageSection

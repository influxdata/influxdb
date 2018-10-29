// Libraries
import React, {Component} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
  fullWidth: boolean
  scrollable: boolean
}

@ErrorHandling
class PageContents extends Component<Props> {
  public render() {
    const {scrollable} = this.props

    if (scrollable) {
      return (
        <FancyScrollbar className="page-contents">
          {this.children}
        </FancyScrollbar>
      )
    }

    return <div className="page-contents">{this.children}</div>
  }

  private get children(): JSX.Element | JSX.Element[] {
    const {fullWidth, children} = this.props

    if (fullWidth) {
      return children
    }

    return (
      <div className="container-fluid">
        <div className="row">{children}</div>
      </div>
    )
  }
}

export default PageContents

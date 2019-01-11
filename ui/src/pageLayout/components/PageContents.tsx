// Libraries
import React, {Component, ReactNode} from 'react'
import classnames from 'classnames'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element | ReactNode
  fullWidth: boolean
  scrollable: boolean
}

@ErrorHandling
class PageContents extends Component<Props> {
  public render() {
    const {scrollable} = this.props

    if (scrollable) {
      return (
        <FancyScrollbar className={this.containerClass}>
          {this.children}
        </FancyScrollbar>
      )
    }

    return <div className={this.containerClass}>{this.children}</div>
  }

  private get containerClass(): string {
    const {fullWidth} = this.props

    return classnames('page-contents', {'full-width': fullWidth})
  }

  private get children(): JSX.Element | JSX.Element[] | ReactNode {
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

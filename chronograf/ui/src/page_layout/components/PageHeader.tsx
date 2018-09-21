// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import PageHeaderLeft from 'src/page_layout/components/PageHeaderLeft'
import PageHeaderRight from 'src/page_layout/components/PageHeaderRight'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
  fullWidth: boolean
}

@ErrorHandling
class PageHeader extends Component<Props> {
  public static Left = PageHeaderLeft
  public static Right = PageHeaderRight

  public render() {
    const {children} = this.props

    return (
      <div className={this.className}>
        <div className="page-header--container">{children}</div>
      </div>
    )
  }

  private get className(): string {
    const {fullWidth} = this.props

    return classnames('page-header', {
      'full-width': fullWidth,
    })
  }
}

export default PageHeader

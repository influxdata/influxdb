// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import PageTitle from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
import PageContents from 'src/reusable_ui/components/page_layout/PageContents'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
  className?: string
}

@ErrorHandling
class Page extends Component<Props> {
  public static Header = PageHeader
  public static Title = PageTitle
  public static Contents = PageContents

  public render() {
    return <div className={this.className}>{this.props.children}</div>
  }

  private get className(): string {
    const {className} = this.props

    return classnames('page', {
      [`${className}`]: className,
    })
  }
}

export default Page

// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import PageHeader from 'src/page_layout/components/PageHeader'
import PageTitle from 'src/page_layout/components/PageTitle'
import PageContents from 'src/page_layout/components/PageContents'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element | JSX.Element[]
  className?: string
}

@ErrorHandling
class Page extends Component<Props> {
  public static Header = PageHeader
  public static Title = PageTitle
  public static Contents = PageContents

  public render() {
    const {children} = this.props

    return <div className={this.className}>{children}</div>
  }

  private get className(): string {
    const {className} = this.props

    return classnames('page', {
      [`${className}`]: className,
    })
  }
}

export default Page

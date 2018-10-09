// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import PageHeaderLeft from 'src/pageLayout/components/PageHeaderLeft'
import PageHeaderCenter from 'src/pageLayout/components/PageHeaderCenter'
import PageHeaderRight from 'src/pageLayout/components/PageHeaderRight'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
  fullWidth: boolean
  inPresentationMode?: boolean
}

@ErrorHandling
class PageHeader extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    inPresentationMode: false,
  }

  public static Left = PageHeaderLeft
  public static Center = PageHeaderCenter
  public static Right = PageHeaderRight

  public render() {
    const {inPresentationMode} = this.props

    if (inPresentationMode) {
      return null
    }

    return (
      <div className={this.className}>
        <div className="page-header--container">{this.children}</div>
      </div>
    )
  }

  private get className(): string {
    const {fullWidth} = this.props

    return classnames('page-header', {
      'full-width': fullWidth,
    })
  }

  private childTypeIsValid = (child: JSX.Element): boolean =>
    child.type === PageHeaderLeft ||
    child.type === PageHeaderCenter ||
    child.type === PageHeaderRight

  private get children(): JSX.Element[] {
    const {children} = this.props

    if (React.Children.count(children) === 0) {
      throw new Error(
        '<Page.Header> requires 1 child of each type: <Page.Header.Left> and <Page.Header.Right>'
      )
    }

    React.Children.forEach(children, (child: JSX.Element) => {
      if (!this.childTypeIsValid(child)) {
        throw new Error(
          '<Page.Header> expected children of type <Page.Header.Left>, <Page.Header.Center>, or <Page.Header.Right>'
        )
      }
    })

    let leftChildCount = 0
    let centerChildCount = 0
    let rightChildCount = 0
    let centerWidthPixels = 0

    React.Children.forEach(children, (child: JSX.Element) => {
      if (child.type === PageHeaderLeft) {
        leftChildCount += 1
      }

      if (child.type === PageHeaderCenter) {
        centerChildCount += 1
        centerWidthPixels = child.props.widthPixels
      }

      if (child.type === PageHeaderRight) {
        rightChildCount += 1
      }
    })

    if (leftChildCount > 1 || centerChildCount > 1 || rightChildCount > 1) {
      throw new Error(
        '<Page.Header> expects at most 1 of each child type: <Page.Header.Left>, <Page.Header.Center>, or <Page.Header.Right>'
      )
    }

    if (leftChildCount === 0 || rightChildCount === 0) {
      throw new Error(
        '<Page.Header> requires 1 child of each type: <Page.Header.Left> and <Page.Header.Right>'
      )
    }

    if (centerWidthPixels) {
      return React.Children.map(children, (child: JSX.Element) => {
        if (child.type === PageHeaderLeft) {
          return (
            <PageHeaderLeft
              {...child.props}
              offsetPixels={centerWidthPixels / 2}
            />
          )
        }

        if (child.type === PageHeaderRight) {
          return (
            <PageHeaderRight
              {...child.props}
              offsetPixels={centerWidthPixels / 2}
            />
          )
        }

        return <PageHeaderCenter {...child.props} />
      })
    }

    return children
  }
}

export default PageHeader

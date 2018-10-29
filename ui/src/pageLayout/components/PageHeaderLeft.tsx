// Libraries
import React, {Component, CSSProperties} from 'react'

// Constants
import {LEFT_MIN_CHILD_COUNT, DEFAULT_OFFSET} from 'src/pageLayout/constants'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element | string | number
  offsetPixels?: number
}

@ErrorHandling
class PageHeaderLeft extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    offsetPixels: DEFAULT_OFFSET,
  }

  public render() {
    const {children} = this.props

    this.validateChildCount()

    return (
      <div className="page-header--left" style={this.styles}>
        {children}
      </div>
    )
  }

  private validateChildCount = (): void => {
    const {children} = this.props

    if (React.Children.count(children) < LEFT_MIN_CHILD_COUNT) {
      throw new Error(
        'Page.Header.Left require at least 1 child element. We recommend using <Page.Title />'
      )
    }
  }

  private get styles(): CSSProperties {
    const {offsetPixels} = this.props

    if (offsetPixels === DEFAULT_OFFSET) {
      return {
        flex: `1 0 ${offsetPixels}`,
      }
    }

    return {
      flex: `1 0 calc(50% - ${offsetPixels}px)`,
    }
  }
}

export default PageHeaderLeft

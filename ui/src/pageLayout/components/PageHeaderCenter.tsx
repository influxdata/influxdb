// Libraries
import React, {Component, CSSProperties} from 'react'

// Constants
import {
  DEFAULT_PAGE_HEADER_CENTER_WIDTH,
  CENTER_MIN_CHILD_COUNT,
} from 'src/pageLayout/constants'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element | string | number
  widthPixels?: number
}

@ErrorHandling
class PageHeaderCenter extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    widthPixels: DEFAULT_PAGE_HEADER_CENTER_WIDTH,
  }

  public render() {
    const {children} = this.props

    this.validateChildCount()

    return (
      <div className="page-header--center" style={this.styles}>
        {children}
      </div>
    )
  }

  private validateChildCount = (): void => {
    const {children} = this.props

    if (React.Children.count(children) < CENTER_MIN_CHILD_COUNT) {
      throw new Error(
        'Page.Header.Left require at least 1 child element. We recommend using <Page.Title />'
      )
    }
  }

  private get styles(): CSSProperties {
    const {widthPixels} = this.props

    return {
      flex: `1 0 ${widthPixels}px`,
    }
  }
}

export default PageHeaderCenter

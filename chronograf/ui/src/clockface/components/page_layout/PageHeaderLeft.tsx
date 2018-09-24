// Libraries
import React, {Component, CSSProperties} from 'react'

// Constants
const DEFAULT_OFFSET = 0
const MIN_CHILD_COUNT = 1

interface Props {
  children: JSX.Element[] | JSX.Element | string | number
  offsetPixels?: number
}

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

    if (React.Children.count(children) < MIN_CHILD_COUNT) {
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

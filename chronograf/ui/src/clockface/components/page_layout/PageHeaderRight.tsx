// Libraries
import React, {Component, CSSProperties} from 'react'

// Constants
const DEFAULT_OFFSET = 0

interface Props {
  children?: JSX.Element[] | JSX.Element | string | number
  offsetPixels?: number
}

class PageHeaderRight extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    offsetPixels: DEFAULT_OFFSET,
  }

  public render() {
    const {children} = this.props

    return (
      <div className="page-header--right" style={this.styles}>
        {children}
      </div>
    )
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

export default PageHeaderRight

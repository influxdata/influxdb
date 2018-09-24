// Libraries
import React, {Component, CSSProperties} from 'react'

// Components
import SourceIndicator from 'src/shared/components/SourceIndicator'

// Constants
const DEFAULT_OFFSET = 0

interface Props {
  children?: JSX.Element[] | JSX.Element | string | number
  offsetPixels?: number
  showSourceIndicator?: boolean
}

class PageHeaderRight extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    offsetPixels: DEFAULT_OFFSET,
    showSourceIndicator: false,
  }

  public render() {
    const {children} = this.props

    return (
      <div className="page-header--right" style={this.styles}>
        {this.sourceIndicator}
        {children}
      </div>
    )
  }

  private get sourceIndicator(): JSX.Element {
    const {showSourceIndicator} = this.props

    if (showSourceIndicator) {
      return <SourceIndicator />
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

export default PageHeaderRight

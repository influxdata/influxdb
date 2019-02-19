import React, {Component, ReactNode, CSSProperties} from 'react'

interface Props {
  children: ReactNode
  maxWidth?: number
}

class OverlayContainer extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    maxWidth: 800,
  }

  public render() {
    const {children} = this.props

    return (
      <div
        className="overlay--container"
        data-testid="overlay--container"
        style={this.style}
      >
        {children}
      </div>
    )
  }

  private get style(): CSSProperties {
    const {maxWidth} = this.props

    return {maxWidth: `${maxWidth}px`}
  }
}

export default OverlayContainer

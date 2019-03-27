import React, {Component, ReactNode, CSSProperties} from 'react'
import classnames from 'classnames'

interface Props {
  children: ReactNode
  maxWidth: number
  customClass?: string
}

class OverlayContainer extends Component<Props> {
  public static defaultProps = {
    maxWidth: 800,
  }

  public render() {
    const {children, customClass} = this.props

    return (
      <div
        className={classnames('overlay--container', {
          [`${customClass}`]: customClass,
        })}
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

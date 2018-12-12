// Libraries
import React, {Component, CSSProperties} from 'react'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element
  minSizePixels?: number
  sizePercent?: number
}

@ErrorHandling
class DraggableResizerPanel extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    minSizePixels: 0,
  }

  public render() {
    const {children} = this.props

    return (
      <div className="draggable-resizer--panel" style={this.style}>
        {children}
      </div>
    )
  }

  private get style(): CSSProperties {
    const {sizePercent, minSizePixels} = this.props

    if (sizePercent) {
      return {flex: `${sizePercent} 0 ${minSizePixels}px`}
    }
  }
}

export default DraggableResizerPanel

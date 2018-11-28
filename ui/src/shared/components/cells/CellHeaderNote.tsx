// Libraries
import React, {PureComponent, CSSProperties} from 'react'

// Components
import CellHeaderNoteTooltip from 'src/shared/components/cells/CellHeaderNoteTooltip'

const MAX_TOOLTIP_WIDTH = 400
const MAX_TOOLTIP_HEIGHT = 200

interface Props {
  note: string
}

interface State {
  isShowingTooltip: boolean
  domRect?: DOMRect
}

class CellHeaderNote extends PureComponent<Props, State> {
  public state: State = {isShowingTooltip: false}

  public render() {
    const {note} = this.props
    const {isShowingTooltip} = this.state

    return (
      <div
        className="cell-header-note"
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        <span className="icon chat" />
        {isShowingTooltip && (
          <CellHeaderNoteTooltip
            note={note}
            containerStyle={this.tooltipStyle}
            maxWidth={MAX_TOOLTIP_WIDTH}
            maxHeight={MAX_TOOLTIP_HEIGHT}
          />
        )}
      </div>
    )
  }

  private get tooltipStyle(): CSSProperties {
    const {x, y, width, height} = this.state.domRect
    const overflowsBottom = y + MAX_TOOLTIP_HEIGHT > window.innerHeight
    const overflowsRight = x + MAX_TOOLTIP_WIDTH > window.innerWidth

    const style: CSSProperties = {}

    if (overflowsBottom) {
      style.bottom = `${window.innerHeight - y - height}px`
    } else {
      style.top = `${y}px`
    }

    if (overflowsRight) {
      style.right = `${window.innerWidth - x}px`
    } else {
      style.left = `${x + width}px`
    }

    return style
  }

  private handleMouseEnter = e => {
    this.setState({
      isShowingTooltip: true,
      domRect: e.target.getBoundingClientRect(),
    })
  }

  private handleMouseLeave = () => {
    this.setState({isShowingTooltip: false})
  }
}

export default CellHeaderNote

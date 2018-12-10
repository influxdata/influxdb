// Libraries
import React, {Component, MouseEvent} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

interface Props {
  children: JSX.Element[] | JSX.Element
}

class RightClickMenu extends Component<Props> {
  public render() {
    return (
      <div className="right-click--menu" onContextMenu={this.handleRightClick}>
        <FancyScrollbar autoHeight={true} maxHeight={250}>
          <ul className="right-click--list">{this.props.children}</ul>
        </FancyScrollbar>
      </div>
    )
  }

  private handleRightClick = (e: MouseEvent<HTMLDivElement>) => {
    e.preventDefault()
  }
}

export default RightClickMenu

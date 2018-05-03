import React, {PureComponent, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const NOOP = () => {}

interface Props {
  id: string
  name?: string
  minPixels: number
  size: number
  activeHandleID: string
  draggable: boolean
  orientation: string
  render: () => ReactElement<any>
  onHandleStartDrag: (id: string, e: MouseEvent<HTMLElement>) => void
  onDoubleClick: (id: string) => void
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
  }

  public render() {
    const {name, render} = this.props
    return (
      <>
        <div className="threesizer--division" style={this.containerStyle}>
          <div
            draggable={true}
            className={this.className}
            onDragStart={this.drag}
            onDoubleClick={this.handleDoubleClick}
          >
            {name}
          </div>
          <FancyScrollbar className="threesizer--contents" autoHide={true}>
            {render()}
          </FancyScrollbar>
        </div>
      </>
    )
  }

  private get containerStyle() {
    return {
      height: `calc((100% - 90px) * ${this.props.size} + 30px)`,
    }
  }

  private get className(): string {
    const {draggable, id, activeHandleID} = this.props

    return classnames('threesizer--handle', {
      disabled: !draggable,
      dragging: id === activeHandleID,
    })
  }

  private drag = e => {
    const {draggable, id} = this.props

    if (!draggable) {
      return NOOP
    }

    this.props.onHandleStartDrag(id, e)
  }

  private handleDoubleClick = () => {
    const {onDoubleClick, id} = this.props

    onDoubleClick(id)
  }
}

export default Division

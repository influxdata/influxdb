import React, {PureComponent, ReactElement, MouseEvent} from 'react'

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
  onHandleStartDrag: (id: string, e: MouseEvent<HTMLElement>) => any
  maxPercent: number
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
  }

  public render() {
    const {name, render} = this.props
    return (
      <>
        <div className="threesizer--division" style={this.style}>
          <div className="threesizer--handle" onMouseDown={this.dragCallback}>
            {name}
          </div>
          {render()}
        </div>
      </>
    )
  }

  private get style() {
    return {
      height: `calc((100% - 90px) * ${this.props.size} + 30px)`,
    }
  }

  private dragCallback = e => {
    const {draggable, id} = this.props
    if (!draggable) {
      return NOOP
    }

    return this.props.onHandleStartDrag(id, e)
  }
}

export default Division

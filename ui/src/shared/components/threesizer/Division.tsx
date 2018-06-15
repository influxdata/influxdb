import React, {
  CSSProperties,
  PureComponent,
  ReactElement,
  MouseEvent,
} from 'react'
import classnames from 'classnames'
import calculateSize from 'calculate-size'

import DivisionHeader from 'src/shared/components/threesizer/DivisionHeader'
import {
  HANDLE_VERTICAL,
  HANDLE_HORIZONTAL,
  MIN_HANDLE_PIXELS,
} from 'src/shared/constants/index'
import {MenuItem} from 'src/shared/components/threesizer/DivisionMenu'

const NOOP = () => {}

interface Props {
  name?: string
  handleDisplay?: string
  menuOptions?: MenuItem[]
  style?: CSSProperties
  handlePixels: number
  id: string
  size: number
  offset: number
  draggable: boolean
  orientation: string
  activeHandleID: string
  headerOrientation: string
  render: (visibility: string) => ReactElement<any>
  onHandleStartDrag: (id: string, e: MouseEvent<HTMLElement>) => void
  onDoubleClick: (id: string) => void
  onMaximize: (id: string) => void
  onMinimize: (id: string) => void
  headerButtons: JSX.Element[]
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
    handleDisplay: 'visible',
    style: {},
  }

  private collapseThreshold: number = 0
  private ref: React.RefObject<HTMLDivElement>

  constructor(props) {
    super(props)
    this.ref = React.createRef<HTMLDivElement>()
  }

  public componentDidMount() {
    const {name} = this.props

    if (!name) {
      return 0
    }

    const {width} = calculateSize(name, {
      font: '"Roboto", Helvetica, Arial, Tahoma, Verdana, sans-serif',
      fontSize: '16px',
      fontWeight: '500',
    })
    const NAME_OFFSET = 96

    this.collapseThreshold = width + NAME_OFFSET
  }

  public render() {
    const {render} = this.props
    return (
      <div
        className={this.containerClass}
        style={this.containerStyle}
        ref={this.ref}
      >
        {this.renderDragHandle}
        <div className={this.contentsClass} style={this.contentStyle}>
          {this.renderHeader}
          <div className="threesizer--body">{render(this.visibility)}</div>
        </div>
      </div>
    )
  }

  private get renderHeader(): JSX.Element {
    const {name, headerButtons, menuOptions, orientation} = this.props

    if (!name) {
      return null
    }

    if (orientation === HANDLE_VERTICAL) {
      return (
        <DivisionHeader
          buttons={headerButtons}
          menuOptions={menuOptions}
          onMinimize={this.handleMinimize}
          onMaximize={this.handleMaximize}
        />
      )
    }
  }

  private get visibility(): string {
    if (this.props.size === 0) {
      return 'hidden'
    }

    return 'visible'
  }

  private get title(): string {
    return 'Drag to resize.\nDouble click to expand.'
  }

  private get contentStyle(): CSSProperties {
    if (this.props.orientation === HANDLE_HORIZONTAL) {
      return {
        height: `calc(100% - ${this.handlePixels}px)`,
      }
    }

    return {
      width: `calc(100% - ${this.handlePixels}px)`,
    }
  }

  private get renderDragHandle(): JSX.Element {
    const {draggable} = this.props

    return (
      <div
        style={this.handleStyle}
        title={this.title}
        draggable={draggable}
        onDragStart={this.drag}
        className={this.handleClass}
        onDoubleClick={this.handleDoubleClick}
      >
        {this.renderDragHandleContents}
      </div>
    )
  }

  private get renderDragHandleContents(): JSX.Element {
    const {
      name,
      handlePixels,
      orientation,
      headerButtons,
      menuOptions,
    } = this.props

    if (!name) {
      return
    }

    if (
      orientation === HANDLE_HORIZONTAL &&
      handlePixels >= MIN_HANDLE_PIXELS
    ) {
      return (
        <DivisionHeader
          buttons={headerButtons}
          menuOptions={menuOptions}
          onMinimize={this.handleMinimize}
          onMaximize={this.handleMaximize}
          name={name}
        />
      )
    }

    if (handlePixels >= MIN_HANDLE_PIXELS) {
      return <div className={this.titleClass}>{name}</div>
    }
  }

  private get handleStyle(): CSSProperties {
    const {handleDisplay: display, orientation, handlePixels} = this.props

    if (orientation === HANDLE_HORIZONTAL) {
      return {
        display,
        height: `${handlePixels}px`,
      }
    }

    return {
      display,
      width: `${handlePixels}px`,
    }
  }

  private get containerStyle(): CSSProperties {
    const {style, orientation} = this.props
    if (orientation === HANDLE_HORIZONTAL) {
      return {
        ...style,
        height: this.size,
      }
    }

    return {
      ...style,
      width: this.size,
    }
  }

  private get size(): string {
    const {size, offset} = this.props
    return `calc((100% - ${offset}px) * ${size} + ${this.handlePixels}px)`
  }

  private get handlePixels(): number {
    if (this.props.handleDisplay === 'none') {
      return 0
    }

    return this.props.handlePixels
  }

  private get containerClass(): string {
    const {orientation} = this.props
    const isAnyHandleBeingDragged = !!this.props.activeHandleID
    return classnames('threesizer--division', {
      dragging: isAnyHandleBeingDragged,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }

  private get handleClass(): string {
    const {draggable, orientation, name} = this.props

    const collapsed = orientation === HANDLE_VERTICAL && this.isTitleObscured

    return classnames('threesizer--handle', {
      'threesizer--collapsed': collapsed,
      disabled: !draggable,
      dragging: this.isDragging,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
      named: name,
    })
  }

  private get contentsClass(): string {
    const {headerOrientation, size} = this.props
    return classnames(`threesizer--contents ${headerOrientation}`, {
      'no-shadows': !size,
    })
  }

  private get titleClass(): string {
    const {orientation} = this.props

    const collapsed = orientation === HANDLE_VERTICAL && this.isTitleObscured

    return classnames('threesizer--title', {
      'threesizer--collapsed': collapsed,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }

  private get isTitleObscured(): boolean {
    if (this.props.size === 0) {
      return true
    }

    if (!this.ref || this.props.size >= 0.33) {
      return false
    }

    const {width} = this.ref.current.getBoundingClientRect()

    return width <= this.collapseThreshold
  }

  private get isDragging(): boolean {
    const {id, activeHandleID} = this.props
    return id === activeHandleID
  }

  private drag = e => {
    const {draggable, id} = this.props

    if (!draggable) {
      return NOOP
    }

    this.props.onHandleStartDrag(id, e)
  }

  private handleDoubleClick = (): void => {
    const {onDoubleClick, id} = this.props

    onDoubleClick(id)
  }

  private handleMinimize = (): void => {
    const {id, onMinimize} = this.props
    onMinimize(id)
  }

  private handleMaximize = (): void => {
    const {id, onMaximize} = this.props
    onMaximize(id)
  }
}

export default Division

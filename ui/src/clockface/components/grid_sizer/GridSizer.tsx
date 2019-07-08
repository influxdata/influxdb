// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children?: JSX.Element[]
  cellWidth: number
  recalculateFlag: string
  width: number
  wait: number
}

interface State {
  columnStyle: {
    width: string
    paddingBottom: string
    margin: string
  }
}
@ErrorHandling
class GridSizer extends PureComponent<Props, State> {
  public static defaultProps = {
    cellWidth: 150,
    recalculateFlag: '',
    width: null,
    wait: 0,
  }

  private timeoutID: NodeJS.Timer
  private debouncedSizeListener: () => void
  private isComponentMounted: boolean

  constructor(props) {
    super(props)
    this.state = {
      columnStyle: null,
    }

    this.debouncedSizeListener = _.debounce(this.setColumnStyle, 250)
  }

  public componentDidMount() {
    this.isComponentMounted = true
    const {width} = this.props
    this.setColumnStyle()

    if (!width) {
      window.addEventListener('resize', this.debouncedSizeListener, false)
    }
  }

  public componentDidUpdate(prevProps) {
    const {recalculateFlag, wait} = this.props
    if (prevProps.recalculateFlag !== recalculateFlag) {
      this.timeoutID = setTimeout(this.setColumnStyle, wait)
    }
  }

  public componentWillUnmount() {
    this.isComponentMounted = false
    clearInterval(this.timeoutID)
    window.removeEventListener('resize', this.debouncedSizeListener, false)
  }

  public render() {
    return (
      <div id="grid_sizer" className="grid-sizer">
        <div className="grid-sizer--cells">{this.sizeChildren}</div>
      </div>
    )
  }

  private get sizeChildren() {
    const {columnStyle} = this.state
    const {children} = this.props

    if (columnStyle) {
      const wrappedChildren = children.map((child, i) => (
        <div
          key={`grid_cell_${i}`}
          style={columnStyle}
          className="grid-sizer--cell"
        >
          <div className="grid-sizer--content">{child}</div>
        </div>
      ))

      return wrappedChildren
    }

    return children
  }

  private getWidth = () => {
    if (!this.isComponentMounted) {
      return
    }

    const ele = document.getElementById('grid_sizer')
    const computedWidth = window
      .getComputedStyle(ele, null)
      .getPropertyValue('width')

    const widthValue = Number(
      computedWidth.substring(0, computedWidth.length - 2)
    )

    return widthValue
  }

  private setColumnStyle = () => {
    const {cellWidth, width} = this.props
    const actualWidth = width || this.getWidth()
    const columns = Math.round(actualWidth / cellWidth)
    const columnsPercent = 1 / columns
    const calculatedCellWidth = actualWidth * columnsPercent
    const gutterWidth = 4
    const columnStyle = {
      width: `${calculatedCellWidth - gutterWidth}px`,
      margin: `${gutterWidth / 2}px`,
      paddingBottom: `${calculatedCellWidth - gutterWidth}px`,
    }

    if (this.isComponentMounted) {
      this.setState({
        columnStyle,
      })
    }
  }
}

export default GridSizer

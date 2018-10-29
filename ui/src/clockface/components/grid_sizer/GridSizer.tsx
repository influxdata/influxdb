// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children?: JSX.Element[]
  cellWidth?: number
  width?: number
}

interface State {
  columns: number
}
@ErrorHandling
class GridSizer extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    cellWidth: 150,
    width: null,
  }

  constructor(props) {
    super(props)
    this.state = {
      columns: null,
    }
  }

  public listener = () => {
    _.debounce(() => this.setColumns(this.getWidth()), 250)
  }

  public componentDidMount() {
    const {width} = this.props
    const widthValue = width || this.getWidth()
    this.setColumns(widthValue)

    if (!width) {
      window.addEventListener('resize', this.listener, false)
    }
  }

  public componentDidUpdate() {
    const {width} = this.props
    if (width) {
      this.setColumns(width)
    }
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.listener, false)
  }

  public render() {
    return (
      <div id="grid_sizer" className="grid-sizer">
        {this.sizeChildren}
      </div>
    )
  }

  private get sizeChildren() {
    const {columns} = this.state
    const {children} = this.props

    if (columns) {
      const wrappedChildren = children.map((child, i) => (
        <div
          key={`grid_cell_${i}`}
          className={`grid-sizer--cell grid-sizer--col-${columns}`}
        >
          <div className="grid-sizer--content">{child}</div>
        </div>
      ))

      return wrappedChildren
    }

    return children
  }

  private getWidth = () => {
    const ele = document.getElementById('grid_sizer')
    const computedWidth = window
      .getComputedStyle(ele, null)
      .getPropertyValue('width')

    const widthValue = Number(
      computedWidth.substring(0, computedWidth.length - 2)
    )

    return widthValue
  }

  private setColumns = (width: number) => {
    const {cellWidth} = this.props
    const columns = Math.round(width / cellWidth)

    this.setState({
      columns,
    })
  }
}

export default GridSizer

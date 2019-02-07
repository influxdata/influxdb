// Libraries
import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import 'src/clockface/components/grid_sizer/ResponsiveGridSizer.scss'

interface PassedProps {
  children: JSX.Element[]
  columns: number
}

interface DefaultProps {
  gutter?: number
}

type Props = PassedProps & DefaultProps

@ErrorHandling
class ResponsiveGridSizer extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    gutter: 4,
  }

  public render() {
    const {children} = this.props

    const cells = children.map((child, i) => (
      <div
        key={`grid_cell_${i}`}
        style={this.cellStyle}
        className="responsive-grid-sizer--cell"
      >
        <div className="responsive-grid-sizer--content">{child}</div>
      </div>
    ))

    return (
      <div className="responsive-grid-sizer">
        <div
          className="responsive-grid-sizer--cells"
          style={this.containerStyle}
        >
          {cells}
        </div>
      </div>
    )
  }

  private get cellStyle(): CSSProperties {
    const {columns, gutter} = this.props

    const cellPercent = 100 / columns
    const margin = gutter / 2

    return {
      width: `calc(${cellPercent}% - ${gutter}px)`,
      paddingBottom: `calc(${cellPercent}% - ${margin}px)`,
      margin,
    }
  }

  private get containerStyle(): CSSProperties {
    const {gutter} = this.props

    const margin = gutter / 2

    return {
      width: `calc(100% + ${gutter}px)`,
      left: `-${margin}px`,
    }
  }
}

export default ResponsiveGridSizer

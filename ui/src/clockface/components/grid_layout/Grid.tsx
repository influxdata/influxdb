// Libraries
import React, {Component} from 'react'

// Components
import GridRow from 'src/clockface/components/grid_layout/GridRow'
import GridColumn from 'src/clockface/components/grid_layout/GridColumn'
import Select from 'src/clockface/components/Select'

// Styles
import 'src/clockface/components/grid_layout/Grid.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
}

@ErrorHandling
class Grid extends Component<Props> {
  public static Row = GridRow
  public static Column = GridColumn

  public render() {
    const {children} = this.props
    return (
      <div className="grid--container">
        <Select type={GridRow}>{children}</Select>
      </div>
    )
  }
}

export default Grid

import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import GaugeOptions from 'src/dashboards/components/GaugeOptions'
import SingleStatOptions from 'src/dashboards/components/SingleStatOptions'
import AxesOptions from 'src/dashboards/components/AxesOptions'
import TableOptions from 'src/dashboards/components/TableOptions'

import {buildDefaultYLabel} from 'src/shared/presenters'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Axes, Cell, QueryConfig} from 'src/types'

interface Props {
  cell: Cell
  Axes: Axes
  queryConfigs: QueryConfig[]
  staticLegend: boolean
  onResetFocus: () => void
  onToggleStaticLegend: () => void
}

interface State {
  axes: Axes
}

@ErrorHandling
class DisplayOptions extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    const {axes, queryConfigs} = props

    this.state = {
      axes: this.setDefaultLabels(axes, queryConfigs),
    }
  }

  public componentWillReceiveProps(nextProps) {
    const {axes, queryConfigs} = nextProps

    this.setState({axes: this.setDefaultLabels(axes, queryConfigs)})
  }

  public render() {
    return (
      <div className="display-options">
        <GraphTypeSelector />
        {this.renderOptions}
      </div>
    )
  }

  private get renderOptions(): JSX.Element {
    const {
      cell,
      staticLegend,
      onToggleStaticLegend,
      onResetFocus,
      queryConfigs,
    } = this.props

    switch (cell.type) {
      case 'gauge':
        return <GaugeOptions onResetFocus={onResetFocus} />
      case 'single-stat':
        return <SingleStatOptions onResetFocus={onResetFocus} />
      case 'table':
        return (
          <TableOptions
            onResetFocus={onResetFocus}
            queryConfigs={queryConfigs}
          />
        )
      default:
        return (
          <AxesOptions
            onToggleStaticLegend={onToggleStaticLegend}
            staticLegend={staticLegend}
          />
        )
    }
  }

  private setDefaultLabels = (axes, queryConfigs): Axes => {
    if (queryConfigs.length) {
      return {
        ...axes,
        y: {...axes.y, defaultYLabel: buildDefaultYLabel(queryConfigs[0])},
      }
    }

    return axes
  }
}

const mstp = ({cellEditorOverlay}) => ({
  cell: cellEditorOverlay.cell,
  axes: cellEditorOverlay.cell.axes,
})

export default connect(mstp, null)(DisplayOptions)

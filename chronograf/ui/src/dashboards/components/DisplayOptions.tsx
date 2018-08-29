import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import GaugeOptions from 'src/dashboards/components/GaugeOptions'
import SingleStatOptions from 'src/dashboards/components/SingleStatOptions'
import AxesOptions from 'src/dashboards/components/AxesOptions'
import TableOptions from 'src/dashboards/components/TableOptions'

import {buildDefaultYLabel} from 'src/shared/utils/defaultYLabel'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Axes, Cell, QueryConfig} from 'src/types'

interface Props {
  cell: Cell
  Axes: Axes
  queryConfigs: QueryConfig[]
  staticLegend: boolean
  onResetFocus: () => void
  onToggleStaticLegend: (x: boolean) => () => void
}

interface State {
  defaultYLabel: string
}

@ErrorHandling
class DisplayOptions extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      defaultYLabel: this.defaultYLabel,
    }
  }

  public componentDidUpdate(prevProps) {
    const {queryConfigs} = prevProps

    if (!_.isEqual(queryConfigs[0], this.props.queryConfigs[0])) {
      this.setState({defaultYLabel: this.defaultYLabel})
    }
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

    const {defaultYLabel} = this.state

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
            staticLegend={staticLegend}
            defaultYLabel={defaultYLabel}
            decimalPlaces={cell.decimalPlaces}
            onToggleStaticLegend={onToggleStaticLegend}
          />
        )
    }
  }

  private get defaultYLabel(): string {
    const {queryConfigs} = this.props
    if (queryConfigs.length) {
      return buildDefaultYLabel(queryConfigs[0])
    }

    return ''
  }
}

const mstp = ({cellEditorOverlay}) => ({
  cell: cellEditorOverlay.cell,
  axes: cellEditorOverlay.cell.axes,
})

export default connect(mstp, null)(DisplayOptions)

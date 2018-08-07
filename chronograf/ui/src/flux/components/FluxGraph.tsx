// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import Dygraph from 'src/shared/components/Dygraph'

// Utils
import {fluxTablesToDygraph} from 'src/shared/parsing/flux/dygraph'

// Actions
import {setHoverTime as setHoverTimeAction} from 'src/dashboards/actions/v2/hoverTime'

// Constants
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'

// Types
import {FluxTable} from 'src/types'
import {DygraphSeries, DygraphValue} from 'src/types'
import {ViewType} from 'src/types/v2/dashboards'

interface Props {
  data: FluxTable[]
  setHoverTime: (time: string) => void
}

class FluxGraph extends PureComponent<Props> {
  public render() {
    const containerStyle = {
      width: 'calc(100% - 32px)',
      height: 'calc(100% - 16px)',
      position: 'absolute',
    }

    return (
      <div className="yield-node--graph">
        <Dygraph
          type={ViewType.Line}
          labels={this.labels}
          staticLegend={false}
          timeSeries={this.timeSeries}
          colors={DEFAULT_LINE_COLORS}
          dygraphSeries={this.dygraphSeries}
          options={this.options}
          containerStyle={containerStyle}
          handleSetHoverTime={this.props.setHoverTime}
        />
      </div>
    )
  }

  private get options() {
    return {
      axisLineColor: '#383846',
      gridLineColor: '#383846',
    }
  }

  // [time, v1, v2, null, v3]
  // time: [v1, v2, null, v3]
  private get timeSeries(): DygraphValue[][] {
    return fluxTablesToDygraph(this.props.data)
  }

  private get labels(): string[] {
    const {data} = this.props
    const names = data.map(d => d.name)

    return ['time', ...names]
  }

  private get dygraphSeries(): DygraphSeries {
    return {}
  }
}

const mdtp = {
  setHoverTime: setHoverTimeAction,
}

export default connect(null, mdtp)(FluxGraph)

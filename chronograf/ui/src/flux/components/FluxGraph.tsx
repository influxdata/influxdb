// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import Dygraph from 'src/shared/components/dygraph/Dygraph'

// Utils
import {fluxTablesToDygraph} from 'src/shared/parsing/flux/dygraph'

// Actions
import {setHoverTime as setHoverTimeAction} from 'src/dashboards/actions/v2/hoverTime'

// Constants
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'

// Types
import {FluxTable} from 'src/types'
import {DygraphSeries} from 'src/types'
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

    const {dygraphsData, labels} = fluxTablesToDygraph(this.props.data)

    return (
      <div className="yield-node--graph">
        <Dygraph
          type={ViewType.Line}
          labels={labels}
          staticLegend={false}
          timeSeries={dygraphsData}
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

  private get dygraphSeries(): DygraphSeries {
    return {}
  }
}

const mdtp = {
  setHoverTime: setHoverTimeAction,
}

export default connect(null, mdtp)(FluxGraph)

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
import {ViewType} from 'src/types/v2/dashboards'

interface Props {
  data: FluxTable[]
  setHoverTime: (time: string) => void
}

class FluxGraph extends PureComponent<Props> {
  public render() {
    const {dygraphsData, labels} = fluxTablesToDygraph(this.props.data)

    return (
      <div className="yield-node--graph">
        <Dygraph
          labels={labels}
          type={ViewType.Line}
          staticLegend={false}
          dygraphSeries={{}}
          options={this.options}
          timeSeries={dygraphsData}
          colors={DEFAULT_LINE_COLORS}
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
}

const mdtp = {
  setHoverTime: setHoverTimeAction,
}

export default connect(null, mdtp)(FluxGraph)

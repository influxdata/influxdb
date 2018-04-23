import React, {PureComponent} from 'react'
import Dygraph from 'dygraphs'
import {connect} from 'react-redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'src/shared/constants'

interface Props {
  hoverTime: number
  dygraph: Dygraph
  staticLegendHeight: number
}

@ErrorHandling
class Crosshair extends PureComponent<Props> {
  public render() {
    if (!this.isVisible) {
      return null
    }

    return (
      <div className="crosshair-container">
        <div
          className="crosshair"
          style={{
            left: this.crosshairLeft,
            height: this.crosshairHeight,
            width: '1px',
          }}
        />
      </div>
    )
  }

  private get isVisible() {
    const {hoverTime} = this.props

    return hoverTime !== 0
  }

  private get crosshairLeft(): number {
    const {dygraph, hoverTime} = this.props
    const cursorOffset = 16
    return dygraph.toDomXCoord(hoverTime) + cursorOffset
  }

  private get crosshairHeight(): string {
    return `calc(100% - ${this.props.staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`
  }
}

const mapStateToProps = ({dashboardUI, annotations: {mode}}) => ({
  mode,
  hoverTime: +dashboardUI.hoverTime,
})

export default connect(mapStateToProps, null)(Crosshair)

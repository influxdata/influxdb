import React, {PureComponent} from 'react'
import Dygraph from 'dygraphs'
import classnames from 'classnames'
import {connect} from 'react-redux'

import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'src/shared/constants'
import {NULL_HOVER_TIME} from 'src/shared/constants/tableGraph'

interface Props {
  hoverTime: number
  dygraph: Dygraph
  staticLegendHeight: number
}

class Crosshair extends PureComponent<Props> {
  public render() {
    return (
      <div className="crosshair-container">
        <div
          className={classnames('crosshair', {
            hidden: this.isHidden,
          })}
          style={{
            left: this.crosshairLeft,
            height: this.crosshairHeight,
            zIndex: 1999,
          }}
        />
      </div>
    )
  }

  private get crosshairLeft(): number {
    const {dygraph, hoverTime} = this.props

    return Math.round(
      Math.max(-1000, dygraph.toDomXCoord(hoverTime)) || -1000 + 1
    )
  }

  private get crosshairHeight(): string {
    return `calc(100% - ${this.props.staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`
  }

  private get isHidden(): boolean {
    return this.props.hoverTime === +NULL_HOVER_TIME
  }
}

const mapStateToProps = ({dashboardUI, annotations: {mode}}) => ({
  mode,
  hoverTime: +dashboardUI.hoverTime,
})

export default connect(mapStateToProps, null)(Crosshair)

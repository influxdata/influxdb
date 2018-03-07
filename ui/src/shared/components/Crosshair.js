import React, {PropTypes, Component} from 'react'
import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'shared/constants'

class Crosshair extends Component {
  render() {
    const {
      dygraph,
      staticLegendHeight,
      hoverTime,
      handleCrosshairRef,
    } = this.props
    const crosshairleft = Math.round(
      Math.max(-1000, dygraph.toDomXCoord(hoverTime)) || -1000 + 1
    )
    const crosshairHeight = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`
    return (
      <div className="new-crosshair" ref={el => handleCrosshairRef(el)}>
        <div
          className="new-crosshair--crosshair"
          style={{
            left: crosshairleft + 1,
            height: crosshairHeight,
            zIndex: 1999,
          }}
        />
      </div>
    )
  }
}

const {func, number, shape, string} = PropTypes

Crosshair.propTypes = {
  dygraph: shape({}),
  staticLegendHeight: number,
  hoverTime: string,
  handleCrosshairRef: func,
}

export default Crosshair

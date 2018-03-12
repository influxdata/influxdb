import React, {PropTypes, Component} from 'react'
import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'shared/constants'
import classnames from 'classnames'

class Crosshair extends Component {
  render() {
    const {dygraph, staticLegendHeight, hoverTime} = this.props
    const crosshairleft = Math.round(
      Math.max(-1000, dygraph.toDomXCoord(hoverTime)) || -1000 + 1
    )
    const crosshairHeight = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`

    return (
      <div className="crosshair">
        <div
          className={classnames('crosshair--crosshair', {
            hidden: crosshairleft < 0,
          })}
          style={{
            left: crosshairleft,
            height: crosshairHeight,
            zIndex: 1999,
          }}
        />
      </div>
    )
  }
}

const {number, shape, string} = PropTypes

Crosshair.propTypes = {
  dygraph: shape({}),
  staticLegendHeight: number,
  hoverTime: string,
}

export default Crosshair

import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'shared/constants'
import {NULL_HOVER_TIME} from 'shared/constants/tableGraph'

import classnames from 'classnames'

class Crosshair extends PureComponent {
  shouldCompnentUpdate(nextProps) {
    return this.props.hoverTime !== nextProps.hoverTime
  }

  render() {
    const {dygraph, staticLegendHeight, hoverTime} = this.props
    const crosshairLeft = Math.round(
      Math.max(-1000, dygraph.toDomXCoord(hoverTime)) || -1000 + 1
    )
    const crosshairHeight = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`

    const crosshairHidden = hoverTime === NULL_HOVER_TIME

    return (
      <div className="crosshair-container">
        <div
          className={classnames('crosshair', {
            hidden: crosshairHidden,
          })}
          style={{
            left: crosshairLeft,
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

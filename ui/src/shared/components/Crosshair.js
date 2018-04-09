import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'

import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'shared/constants'
import {NULL_HOVER_TIME} from 'shared/constants/tableGraph'

class Crosshair extends PureComponent {
  shouldCompnentUpdate(nextProps) {
    return this.props.hoverTime !== nextProps.hoverTime
  }

  render() {
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

  get crosshairLeft() {
    const {dygraph, hoverTime} = this.props

    return Math.round(
      Math.max(-1000, dygraph.toDomXCoord(hoverTime)) || -1000 + 1
    )
  }

  get crosshairHeight() {
    return `calc(100% - ${this.props.staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`
  }

  get isHidden() {
    return this.props.hoverTime === NULL_HOVER_TIME
  }
}

const {number, shape, string} = PropTypes

Crosshair.propTypes = {
  dygraph: shape({}),
  staticLegendHeight: number,
  hoverTime: string,
}

export default Crosshair

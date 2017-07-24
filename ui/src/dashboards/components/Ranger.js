import React, {PropTypes} from 'react'
import _ from 'lodash'

const Ranger = ({onSetRange, axes}) => {
  const min = _.get(axes, ['y', 'bounds', '0'], '')
  const max = _.get(axes, ['y', 'bounds', '1'], '')

  return (
    <div className="display-options--cell">
      <h5 className="display-options--header">Y Axis Controls</h5>
      <form autoComplete="off">
        <div className="display-options--row">
          <label htmlFor="min" style={{width: '40px'}}>
            Min
          </label>
          <input
            className="form-control input-sm"
            type="number"
            name="min"
            id="min"
            value={min}
            onChange={onSetRange}
            placeholder="auto"
          />
        </div>
        <div className="display-options--row">
          <label htmlFor="max" style={{width: '40px'}}>
            Maximum
          </label>
          <input
            className="form-control input-sm"
            type="number"
            name="max"
            id="max"
            value={max}
            onChange={onSetRange}
            placeholder="auto"
          />
        </div>
      </form>
    </div>
  )
}

const {array, func, shape} = PropTypes

Ranger.propTypes = {
  onSetRange: func.isRequired,
  axes: shape({
    y: shape({
      bounds: array,
    }),
  }).isRequired,
}

export default Ranger

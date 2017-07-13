import React, {PropTypes} from 'react'

const Ranger = ({onSetRange, yRanges}) =>
  <div className="display-options--cell">
    <h5 className="display-options--header">Y Axis Controls</h5>
    <form autoComplete="off">
      <div className="display-options--row">
        <label htmlFor="min" style={{width: '40px'}}>Min</label>
        <input
          className="form-control input-sm"
          type="text"
          name="min"
          id="min"
          value={yRanges.y[0]}
          onChange={onSetRange}
          placeholder="auto"
        />
      </div>
      <div className="display-options--row">
        <label htmlFor="max" style={{width: '40px'}}>Max</label>
        <input
          className="form-control input-sm"
          type="text"
          name="max"
          id="max"
          value={yRanges.y[1]}
          onChange={onSetRange}
          placeholder="auto"
        />
      </div>
    </form>
  </div>

const {array, func, shape} = PropTypes

Ranger.propTypes = {
  onSetRange: func.isRequired,
  yRanges: shape({
    y: array,
    y2: array,
  }).isRequired,
}

export default Ranger

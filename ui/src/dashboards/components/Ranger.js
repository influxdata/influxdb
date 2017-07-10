import React, {PropTypes} from 'react'

const Ranger = ({onSetRange, yRange}) => (
  <div className="yRanger" style={{display: 'flex'}}>
    <form onSubmit={onSetRange} autoComplete="off">
      <label htmlFor="min">Minimum</label>
      <input
        className="form-control input-sm"
        type="text"
        name="min"
        id="min"
        value={yRange.min}
        onChange={onSetRange}
      />
      <label htmlFor="max">Maximum</label>
      <input
        className="form-control input-sm"
        type="text"
        name="max"
        id="max"
        value={yRange.max}
        onChange={onSetRange}
      />
    </form>
  </div>
)

const {func, shape, string} = PropTypes

Ranger.propTypes = {
  onSetRange: func.isRequired,
  yRange: shape({
    min: string,
    max: string,
  }).isRequired,
}

export default Ranger

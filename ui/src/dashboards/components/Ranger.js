import React, {PropTypes} from 'react'

const Ranger = ({onSetRange, yRange}) =>
  <div className="yRanger">
    <form onSubmit={onSetRange}>
      <label htmlFor="min">Min</label>
      <input
        type="text"
        name="min"
        id="min"
        value={yRange.min}
        onChange={onSetRange}
      />
      <label htmlFor="max">Max</label>
      <input
        type="text"
        name="max"
        id="max"
        value={yRange.max}
        onChange={onSetRange}
      />
    </form>
  </div>

const {func, shape, string} = PropTypes

Ranger.propTypes = {
  onSetRange: func.isRequired,
  yRange: shape({
    min: string,
    max: string,
  }).isRequired,
}

export default Ranger

import React, {PropTypes} from 'react'
import _ from 'lodash'

// TODO: add logic for for Prefix, Suffix, Scale, and Multiplier
const Ranger = ({onSetRange, axes}) => {
  const min = _.get(axes, ['y', 'bounds', '0'], '')
  const max = _.get(axes, ['y', 'bounds', '1'], '')

  return (
    <div className="display-options--cell">
      <h5 className="display-options--header">Y Axis Controls</h5>
      <form autoComplete="off" style={{margin: '0 -6px'}}>
        {/* <div className="form-group col-sm-12">
          <label htmlFor="prefix">Axis Title</label>
          <input
            className="form-control input-sm"
            type="text"
            name="label"
            id="label"
          />
        </div> */}
        <div className="form-group col-sm-6">
          <label htmlFor="min">Lower Bound</label>
          <div className="one-or-any">
            <div className="one-or-any--auto">auto</div>
            <div className="one-or-any--switch">
              <div />
            </div>
            <input
              className="form-control input-sm"
              type="number"
              name="min"
              id="min"
              value={min}
              onChange={onSetRange}
              placeholder="Custom Value"
              disabled={true}
            />
          </div>
        </div>
        <div className="form-group col-sm-6">
          <label htmlFor="max">Upper Bound</label>
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
        {/* <div className="form-group col-sm-6">
          <label htmlFor="prefix">Labels Prefix</label>
          <input
            className="form-control input-sm"
            type="text"
            name="prefix"
            id="prefix"
          />
        </div>
        <div className="form-group col-sm-6">
          <label htmlFor="prefix">Labels Suffix</label>
          <input
            className="form-control input-sm"
            type="text"
            name="suffix"
            id="suffix"
          />
        </div>
        <div className="form-group col-sm-6">
          <label>Labels Format</label>
          <ul className="nav nav-tablist nav-tablist-sm">
            <li className="active">K/M/B</li>
            <li>K/M/G</li>
          </ul>
        </div>
        <div className="form-group col-sm-6">
          <label>Scale</label>
          <ul className="nav nav-tablist nav-tablist-sm">
            <li className="active">Linear</li>
            <li>Logarithmic</li>
          </ul>
        </div> */}
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

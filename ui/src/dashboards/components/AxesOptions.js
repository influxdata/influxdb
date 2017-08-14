import React, {PropTypes} from 'react'
import _ from 'lodash'

// TODO: add logic for for Prefix, Suffix, Scale, and Multiplier
const AxesOptions = ({onSetRange, onSetLabel, axes}) => {
  const min = _.get(axes, ['y', 'bounds', '0'], '')
  const max = _.get(axes, ['y', 'bounds', '1'], '')
  const label = _.get(axes, ['y', 'label'], '')

  return (
    <div className="display-options--cell">
      <h5 className="display-options--header">Y Axis Controls</h5>
      <form autoComplete="off" style={{margin: '0 -6px'}}>
        <div className="form-group col-sm-12">
          <label htmlFor="prefix">Title</label>
          <input
            className="form-control input-sm"
            type="text"
            name="label"
            id="label"
            value={label}
            onChange={onSetLabel}
            placeholder="auto"
          />
        </div>
        <div className="form-group col-sm-6">
          <label htmlFor="min">Min</label>
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
        <div className="form-group col-sm-6">
          <label htmlFor="max">Max</label>
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
        <p className="display-options--footnote">
          Values left blank will be set automatically
        </p>
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

const {arrayOf, func, shape, string} = PropTypes

AxesOptions.propTypes = {
  onSetRange: func.isRequired,
  onSetLabel: func.isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
      label: string,
    }),
  }).isRequired,
}

export default AxesOptions

import React, {PropTypes} from 'react'
import _ from 'lodash'

import OptIn from 'shared/components/OptIn'

// TODO: add logic for for Prefix, Suffix, Scale, and Multiplier
const AxesOptions = ({
  onSetYAxisBoundMin,
  onSetYAxisBoundMax,
  onSetLabel,
  axes,
}) => {
  const min = _.get(axes, ['y', 'bounds', '0'], '')
  const max = _.get(axes, ['y', 'bounds', '1'], '')
  const label = _.get(axes, ['y', 'label'], '')
  const defaultYLabel = _.get(axes, ['y', 'defaultYLabel'], '')

  return (
    <div className="display-options--cell">
      <h5 className="display-options--header">Y Axis Controls</h5>
      <form autoComplete="off" style={{margin: '0 -6px'}}>
        <div className="form-group col-sm-12">
          <label htmlFor="prefix">Title</label>
          <OptIn
            customPlaceholder={defaultYLabel}
            customValue={label}
            onSetValue={onSetLabel}
            type="text"
          />
        </div>
        <div className="form-group col-sm-6">
          <label htmlFor="min">Min</label>
          <OptIn
            customPlaceholder={'min'}
            customValue={min}
            onSetValue={onSetYAxisBoundMin}
            type="number"
          />
        </div>
        <div className="form-group col-sm-6">
          <label htmlFor="max">Max</label>
          <OptIn
            customPlaceholder={'max'}
            customValue={max}
            onSetValue={onSetYAxisBoundMax}
            type="number"
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

const {arrayOf, func, shape, string} = PropTypes

AxesOptions.propTypes = {
  onSetYAxisBoundMin: func.isRequired,
  onSetYAxisBoundMax: func.isRequired,
  onSetLabel: func.isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
      label: string,
      defaultYLabel: string,
    }),
  }).isRequired,
}

export default AxesOptions

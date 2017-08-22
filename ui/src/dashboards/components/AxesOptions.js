import React, {PropTypes} from 'react'

import OptIn from 'shared/components/OptIn'

const AxesOptions = ({
  axes: {y: {bounds, label, prefix, suffix, base, defaultYLabel}},
  onSetBase,
  onSetLabel,
  onSetPrefixSuffix,
  onSetYAxisBoundMin,
  onSetYAxisBoundMax,
}) => {
  const [min, max] = bounds

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
        <div className="form-group col-sm-6">
          <label htmlFor="prefix">Y-Value's Prefix</label>
          <input
            className="form-control input-sm"
            type="text"
            name="prefix"
            id="prefix"
            value={prefix}
            onChange={onSetPrefixSuffix}
          />
        </div>
        <div className="form-group col-sm-6">
          <label htmlFor="prefix">Y-Value's Suffix</label>
          <input
            className="form-control input-sm"
            type="text"
            name="suffix"
            id="suffix"
            value={suffix}
            onChange={onSetPrefixSuffix}
          />
        </div>
        <div className="form-group col-sm-6">
          <label>Labels Format</label>
          <ul className="nav nav-tablist nav-tablist-sm">
            <li
              className={base === '10' ? 'active' : ''}
              onClick={onSetBase('10')}
            >
              K/M/B
            </li>
            <li
              className={base === '2' ? 'active' : ''}
              onClick={onSetBase('2')}
            >
              K/M/G
            </li>
          </ul>
        </div>
        <div className="form-group col-sm-6">
          <label>Scale</label>
          <ul className="nav nav-tablist nav-tablist-sm">
            <li className="active">Linear</li>
            <li>Logarithmic</li>
          </ul>
        </div>
      </form>
    </div>
  )
}

const {arrayOf, func, shape, string} = PropTypes

AxesOptions.defaultProps = {
  axes: {
    y: {
      bounds: ['', ''],
      prefix: '',
      suffix: '',
      base: '10',
      scale: 'linear',
      defaultYLabel: '',
    },
  },
}

AxesOptions.propTypes = {
  onSetPrefixSuffix: func.isRequired,
  onSetYAxisBoundMin: func.isRequired,
  onSetYAxisBoundMax: func.isRequired,
  onSetLabel: func.isRequired,
  onSetBase: func.isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
      label: string,
      defaultYLabel: string,
    }),
  }).isRequired,
}

export default AxesOptions

import React, {PropTypes} from 'react'

import OptIn from 'shared/components/OptIn'
import Input from 'src/dashboards/components/DisplayOptionsInput'
import {Tabber, Tab} from 'src/dashboards/components/Tabber'
import {DISPLAY_OPTIONS, TOOLTIP_CONTENT} from 'src/dashboards/constants'

const {LINEAR, LOG, BASE_2, BASE_10} = DISPLAY_OPTIONS

const AxesOptions = ({
  axes: {y: {bounds, label, prefix, suffix, base, scale, defaultYLabel}},
  onSetBase,
  onSetScale,
  onSetLabel,
  onSetPrefixSuffix,
  onSetYAxisBoundMin,
  onSetYAxisBoundMax,
}) => {
  const [min, max] = bounds

  return (
    <div className="display-options--cell y-axis-controls">
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
        <Input
          name="prefix"
          id="prefix"
          value={prefix}
          labelText="Y-Value's Prefix"
          onChange={onSetPrefixSuffix}
        />
        <Input
          name="suffix"
          id="suffix"
          value={suffix}
          labelText="Y-Value's Suffix"
          onChange={onSetPrefixSuffix}
        />
        <Tabber
          labelText="Y-Value's Format"
          tipID="Y-Values's Format"
          tipContent={TOOLTIP_CONTENT.FORMAT}
        >
          <Tab
            text="K/M/B"
            isActive={base === BASE_10}
            onClickTab={onSetBase(BASE_10)}
          />
          <Tab
            text="K/M/G"
            isActive={base === BASE_2}
            onClickTab={onSetBase(BASE_2)}
          />
        </Tabber>
        <Tabber labelText="Scale">
          <Tab
            text="Linear"
            isActive={scale === LINEAR}
            onClickTab={onSetScale(LINEAR)}
          />
          <Tab
            text="Logarithmic"
            isActive={scale === LOG}
            onClickTab={onSetScale(LOG)}
          />
        </Tabber>
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
      base: BASE_10,
      scale: LINEAR,
      defaultYLabel: '',
    },
  },
}

AxesOptions.propTypes = {
  onSetPrefixSuffix: func.isRequired,
  onSetYAxisBoundMin: func.isRequired,
  onSetYAxisBoundMax: func.isRequired,
  onSetLabel: func.isRequired,
  onSetScale: func.isRequired,
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

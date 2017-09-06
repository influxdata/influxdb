import React, {PropTypes} from 'react'
import {OPERATORS} from 'src/kapacitor/constants'
import Dropdown from 'shared/components/Dropdown'

const mapToItems = (arr, type) => arr.map(text => ({text, type}))
const operators = mapToItems(OPERATORS, 'operator')

const Threshold = ({
  rule: {values: {operator, value, rangeValue}},
  query,
  onDropdownChange,
  onThresholdInputChange,
}) =>
  <div className="rule-section--row rule-section--border-bottom">
    <p>Send Alert where</p>
    <span className="rule-builder--metric">
      {query.fields.length ? query.fields[0].field : 'Select a Time-Series'}
    </span>
    <p>is</p>
    <Dropdown
      className="dropdown-180"
      menuClass="dropdown-malachite"
      items={operators}
      selected={operator}
      onChoose={onDropdownChange}
    />
    <form style={{display: 'flex'}}>
      <input
        className="form-control input-sm form-malachite monotype"
        style={{width: '160px', marginLeft: '6px'}}
        type="text"
        name="lower"
        spellCheck="false"
        value={value}
        onChange={onThresholdInputChange}
        placeholder={
          operator === 'inside range' || operator === 'outside range'
            ? 'Lower'
            : null
        }
      />
      {(operator === 'inside range' || operator === 'outside range') &&
        <input
          className="form-control input-sm form-malachite monotype"
          name="upper"
          style={{width: '160px'}}
          placeholder="Upper"
          type="text"
          spellCheck="false"
          value={rangeValue}
          onChange={onThresholdInputChange}
        />}
    </form>
  </div>

const {shape, string, func} = PropTypes

Threshold.propTypes = {
  rule: shape({
    values: shape({
      operator: string,
      rangeOperator: string,
      value: string,
      rangeValue: string,
    }),
  }),
  onDropdownChange: func.isRequired,
  onThresholdInputChange: func.isRequired,
  query: shape({}).isRequired,
}

export default Threshold

import React, {PropTypes} from 'react'
import {OPERATORS} from 'src/kapacitor/constants'
import Dropdown from 'shared/components/Dropdown'

const mapToItems = (arr, type) => arr.map(text => ({text, type}))

const Threshold = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        operator: PropTypes.string,
        rangeOperator: PropTypes.string,
        value: PropTypes.string,
        rangeValue: PropTypes.string,
      }),
    }),
    onChange: PropTypes.func.isRequired,
    query: PropTypes.shape({}).isRequired,
  },

  handleDropdownChange(item) {
    this.props.onChange({...this.props.rule.values, [item.type]: item.text})
  },

  handleInputChange() {
    this.props.onChange({
      ...this.props.rule.values,
      value: this.valueInput.value,
      rangeValue: this.valueRangeInput ? this.valueRangeInput.value : '',
    })
  },

  render() {
    const {operator, value, rangeValue} = this.props.rule.values
    const {query} = this.props

    const operators = mapToItems(OPERATORS, 'operator')

    return (
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
          onChoose={this.handleDropdownChange}
        />
        <input
          className="form-control input-sm form-malachite monotype"
          style={{width: '160px'}}
          type="text"
          spellCheck="false"
          ref={r => (this.valueInput = r)}
          defaultValue={value}
          onKeyUp={this.handleInputChange}
          placeholder={
            operator === 'inside range' || operator === 'outside range'
              ? 'Lower'
              : null
          }
        />
        {(operator === 'inside range' || operator === 'outside range') &&
          <input
            className="form-control input-sm form-malachite monotype"
            style={{width: '160px'}}
            placeholder="Upper"
            type="text"
            spellCheck="false"
            ref={r => (this.valueRangeInput = r)}
            defaultValue={rangeValue}
            onKeyUp={this.handleInputChange}
          />}
      </div>
    )
  },
})

export default Threshold

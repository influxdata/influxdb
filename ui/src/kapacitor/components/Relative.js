import React, {PropTypes} from 'react'
import {CHANGES, OPERATORS, SHIFTS} from 'src/kapacitor/constants'
import Dropdown from 'shared/components/Dropdown'

const mapToItems = (arr, type) => arr.map(text => ({text, type}))

const Relative = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        change: PropTypes.string,
        shift: PropTypes.string,
        operator: PropTypes.string,
        value: PropTypes.string,
      }),
    }),
    onChange: PropTypes.func.isRequired,
  },

  handleDropdownChange(item) {
    this.props.onChange({...this.props.rule.values, [item.type]: item.text})
  },

  handleInputChange() {
    this.props.onChange({...this.props.rule.values, value: this.input.value})
  },

  render() {
    const {change, shift, operator, value} = this.props.rule.values

    const changes = mapToItems(CHANGES, 'change')
    const shifts = mapToItems(SHIFTS, 'shift')
    const operators = mapToItems(OPERATORS, 'operator')

    return (
      <div className="rule-section--row rule-section--border-bottom">
        <p>Send Alert when</p>
        <Dropdown
          className="dropdown-110"
          menuClass="dropdown-malachite"
          items={changes}
          selected={change}
          onChoose={this.handleDropdownChange}
        />
        <p>compared to previous</p>
        <Dropdown
          className="dropdown-80"
          menuClass="dropdown-malachite"
          items={shifts}
          selected={shift}
          onChoose={this.handleDropdownChange}
        />
        <p>is</p>
        <Dropdown
          className="dropdown-160"
          menuClass="dropdown-malachite"
          items={operators}
          selected={operator}
          onChoose={this.handleDropdownChange}
        />
        <input
          className="form-control input-sm form-malachite monotype"
          style={{width: '160px'}}
          ref={r => (this.input = r)}
          defaultValue={value}
          onKeyUp={this.handleInputChange}
          required={true}
          type="text"
          spellCheck="false"
        />
        {change === CHANGES[1] ? <p>%</p> : null}
      </div>
    )
  },
})

export default Relative

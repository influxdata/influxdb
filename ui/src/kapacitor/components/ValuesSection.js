import React, {PropTypes} from 'react'

import _ from 'lodash'

import Dropdown from 'shared/components/Dropdown'
import Deadman from 'src/kapacitor/components/Deadman'

import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs'
import {OPERATORS, CHANGES, SHIFTS} from 'src/kapacitor/constants'

const TABS = ['Threshold', 'Relative', 'Deadman']
const mapToItems = (arr, type) => arr.map(text => ({text, type}))

export const ValuesSection = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      id: PropTypes.string,
    }).isRequired,
    onChooseTrigger: PropTypes.func.isRequired,
    onUpdateValues: PropTypes.func.isRequired,
    query: PropTypes.shape({}).isRequired,
    onDeadmanChange: PropTypes.func.isRequired,
  },

  render() {
    const {rule, query, onDeadmanChange} = this.props
    const initialIndex = TABS.indexOf(_.startCase(rule.trigger))

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Rule Conditions</h3>
        <div className="rule-section--body">
          <Tabs initialIndex={initialIndex} onSelect={this.handleChooseTrigger}>
            <TabList isKapacitorTabs="true">
              {TABS.map(tab =>
                <Tab key={tab} isKapacitorTab={true}>
                  {tab}
                </Tab>
              )}
            </TabList>

            <TabPanels>
              <TabPanel>
                <Threshold
                  rule={rule}
                  query={query}
                  onChange={this.handleValuesChange}
                />
              </TabPanel>
              <TabPanel>
                <Relative rule={rule} onChange={this.handleValuesChange} />
              </TabPanel>
              <TabPanel>
                <Deadman rule={rule} onChange={onDeadmanChange} />
              </TabPanel>
            </TabPanels>
          </Tabs>
        </div>
      </div>
    )
  },

  handleChooseTrigger(triggerIndex) {
    const {rule, onChooseTrigger} = this.props
    if (TABS[triggerIndex] === rule.trigger) {
      return
    }

    onChooseTrigger(rule.id, TABS[triggerIndex])
  },

  handleValuesChange(values) {
    const {onUpdateValues, rule} = this.props
    onUpdateValues(rule.id, rule.trigger, values)
  },
})

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

export default ValuesSection

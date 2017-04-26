import React, {PropTypes} from 'react'
import Dropdown from 'src/shared/components/Dropdown'
import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs'
import {OPERATORS, PERIODS, CHANGES, SHIFTS} from 'src/kapacitor/constants'
import _ from 'lodash'

const TABS = ['Threshold', 'Relative', 'Deadman']
const mapToItems = (arr, type) => arr.map((text) => ({text, type}))

export const ValuesSection = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      id: PropTypes.string,
    }).isRequired,
    onChooseTrigger: PropTypes.func.isRequired,
    onUpdateValues: PropTypes.func.isRequired,
    query: PropTypes.shape({}).isRequired,
  },

  render() {
    const {rule, query} = this.props
    const initialIndex = TABS.indexOf(_.startCase(rule.trigger))

    return (
      <div className="kapacitor-rule-section">
        <h3 className="rule-section-heading">Values</h3>
        <div className="rule-section-body">
          <Tabs initialIndex={initialIndex} onSelect={this.handleChooseTrigger}>
            <TabList isKapacitorTabs="true">
              {TABS.map(tab => <Tab key={tab}>{tab}</Tab>)}
            </TabList>

            <TabPanels>
              <TabPanel>
                <Threshold rule={rule} query={query} onChange={this.handleValuesChange} />
              </TabPanel>
              <TabPanel>
                <Relative rule={rule} onChange={this.handleValuesChange} />
              </TabPanel>
              <TabPanel>
                <Deadman rule={rule} onChange={this.handleValuesChange} />
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
      <div className="value-selector">
        <p>Send Alert where</p>
        <span>{query.fields.length ? query.fields[0].field : 'Select a Time-Series'}</span>
        <p>is</p>
        <Dropdown className="size-176 dropdown-kapacitor" items={operators} selected={operator} onChoose={this.handleDropdownChange} />
        <input className="form-control input-sm size-166 form-control--green" type="text" spellCheck="false" ref={(r) => this.valueInput = r} defaultValue={value} onKeyUp={this.handleInputChange} />
        { (operator === 'inside range' || operator === 'outside range') &&
          <input className="form-control input-sm size-166 form-control--green" type="text" spellCheck="false" ref={(r) => this.valueRangeInput = r} defaultValue={rangeValue} onKeyUp={this.handleInputChange} />
        }
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
      <div className="value-selector">
        <p>Send Alert when</p>
        <Dropdown className="size-106 dropdown-kapacitor"items={changes} selected={change} onChoose={this.handleDropdownChange} />
        <p>compared to previous</p>
        <Dropdown className="size-66 dropdown-kapacitor" items={shifts} selected={shift} onChoose={this.handleDropdownChange} />
        <p>is</p>
        <Dropdown className="size-176 dropdown-kapacitor" items={operators} selected={operator} onChoose={this.handleDropdownChange} />
        <input
          className="form-control input-sm size-166 form-control--green"
          ref={(r) => this.input = r}
          defaultValue={value}
          onKeyUp={this.handleInputChange}
          required={true}
          type="text"
          spellCheck="false"
        />
        <p>{ change === CHANGES[1] ? '%' : '' }</p>
      </div>
    )
  },
})

const Deadman = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        period: PropTypes.string,
      }),
    }),
    onChange: PropTypes.func.isRequired,
  },

  handleChange(item) {
    this.props.onChange({period: item.text})
  },

  render() {
    const periods = PERIODS.map((text) => {
      return {text}
    })

    return (
      <div className="value-selector">
        <p>Send Alert if Data is missing for</p>
        <Dropdown className="size-66 dropdown-kapacitor" items={periods} selected={this.props.rule.values.period} onChoose={this.handleChange} />
      </div>
    )
  },
})

export default ValuesSection

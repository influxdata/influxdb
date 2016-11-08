import React, {PropTypes} from 'react';
import Dropdown from 'src/shared/components/Dropdown';
import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs';
import {OPERATORS, PERIODS, CHANGES, SHIFTS} from 'src/kapacitor/constants';

const TABS = ['Threshold', 'Relative', 'Deadman'];
export const ValuesSection = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      id: PropTypes.string,
    }).isRequired,
    onChooseTrigger: PropTypes.func.isRequired,
    onUpdateValues: PropTypes.func.isRequired,
  },

  render() {
    const {rule} = this.props;

    return (
      <div>
        <Tabs onSelect={this.handleChooseTrigger}>
          <TabList isKapacitorTabs="true">
            {TABS.map(tab => <Tab key={tab}>{tab}</Tab>)}
          </TabList>

          <TabPanels>
            <TabPanel>
              <Threshold rule={rule} onChange={this.handleValuesChange} />
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
    );
  },

  handleChooseTrigger(triggerIndex) {
    const {rule, onChooseTrigger} = this.props;
    if (TABS[triggerIndex] === rule.trigger) {
      return;
    }

    onChooseTrigger(rule.id, TABS[triggerIndex]);
  },

  handleValuesChange(values) {
    const {onUpdateValues, rule} = this.props;
    onUpdateValues(rule.id, rule.trigger, values);
  },
});

const Threshold = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        operator: PropTypes.string,
        value: PropTypes.string,
        period: PropTypes.string,
      }),
    }),
    onChange: PropTypes.func.isRequired,
  },

  handleDropdownChange(item) {
    const newValues = Object.assign({}, this.props.rule.values, {[item.type]: item.text});
    this.props.onChange(newValues);
  },

  handleInputChange() {
    this.props.onChange(Object.assign({}, this.props.rule.values, {
      value: this.valueInput.value,
    }));
  },

  render() {
    const {operator, value, period} = this.props.rule.values;

    function mapToItems(arr, type) {
      return arr.map((text) => {
        return {text, type};
      });
    }

    const operators = mapToItems(OPERATORS, 'operator');
    const periods = mapToItems(PERIODS, 'period');

    return (
      <div className="value-selector">
        <p>Send Alert when Value is</p>
        <Dropdown className="size-176" items={operators} selected={operator} onChoose={this.handleDropdownChange} />
        <input className="form-control input-sm size-49" placeholder="000" type="text" ref={(r) => this.valueInput = r} defaultValue={value} onKeyUp={this.handleInputChange}></input>
        <p>during the last</p>
        <Dropdown className="size-66" items={periods} selected={period} onChoose={this.handleDropdownChange} />
      </div>
    );
  },
});

const Relative = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        change: PropTypes.string,
        period: PropTypes.string,
        shift: PropTypes.string,
        operator: PropTypes.string,
        value: PropTypes.string,
      }),
    }),
    onChange: PropTypes.func.isRequired,
  },

  handleDropdownChange(item) {
    this.props.onChange(Object.assign({}, this.props.rule.values, {[item.type]: item.text}));
  },

  handleInputChange() {
    this.props.onChange(Object.assign({}, this.props.rule.values, {value: this.input.value}));
  },

  render() {
    const {change, period, shift, operator, value} = this.props.rule.values;

    function mapToItems(arr, type) {
      return arr.map((text) => {
        return {text, type};
      });
    }

    const changes = mapToItems(CHANGES, 'change');
    const periods = mapToItems(PERIODS, 'period');
    const shifts = mapToItems(SHIFTS, 'shift');
    const operators = mapToItems(OPERATORS, 'operator');

    return (
      <div className="value-selector">
        <p>Send Alert when</p>
        <Dropdown className="size-106"items={changes} selected={change} onChoose={this.handleDropdownChange} />
        <p>over</p>
        <Dropdown className="size-66" items={periods} selected={period} onChoose={this.handleDropdownChange} />
        <p>compared to previous</p>
        <Dropdown className="size-66" items={shifts} selected={shift} onChoose={this.handleDropdownChange} />
        <p>is</p>
        <Dropdown className="size-176" items={operators} selected={operator} onChoose={this.handleDropdownChange} />
        <input className="form-control input-sm size-49" type="text" ref={(r) => this.input = r} defaultValue={value} onKeyUp={this.handleInputChange}></input>
        <p>{ change === CHANGES[1] ? '%' : '' }</p>
      </div>
    );
  },
});

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
    this.props.onChange({period: item.text});
  },

  render() {
    const periods = PERIODS.map((text) => {
      return {text};
    });

    return (
      <div className="value-selector">
        <p>Send Alert if Data is missing for</p>
        <Dropdown className="size-66" items={periods} selected={this.props.rule.values.period} onChoose={this.handleChange} />
      </div>
    );
  },
});

export default ValuesSection;

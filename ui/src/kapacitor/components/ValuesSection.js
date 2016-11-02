import React, {PropTypes} from 'react';
import Dropdown from 'src/shared/components/Dropdown';
import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs';

const TABS = ['Threshold', 'Relative', 'Deadman'];
export const ValuesSection = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      id: PropTypes.string,
    }).isRequired,
    onChooseTrigger: PropTypes.func.isRequired,
    onUpdateValues: PropTypes.func.isRequired,
  },

  // √ when a tab is selected, update the rule.trigger to match
  // when the user interacts with the form associated with a trigger,
  // round up all of the data and overwrite the values in the rule.
  //
  // Then, when save is clicked, the rule will already be in the right state to be sent to the server...
  // We can run validations before we even highlight the save button
  //
  render() {
    const {rule} = this.props;

    return (
      <div className="container-fluid">
        <div className="row">
          <div className="col-md-12">
            <Tabs onSelect={this.handleChooseTrigger}>
              <TabList>
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
        </div>
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
        relation: PropTypes.string,
        percentile: PropTypes.string,
        duration: PropTypes.string,
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
      percentile: this.percentileInput.value,
    }));
  },

  render() {
    const {operator, value, relation, percentile, duration} = this.props.rule.values;

    function mapToItems(arr, type) {
      return arr.map((text) => {
        return {text, type};
      });
    }

    const operators = mapToItems(['greater than', 'less than', 'equal to', 'not equal to'], 'operator');
    const relations = mapToItems(['once', 'more than ', 'less than'], 'relation');
    const durations = mapToItems(['1m', '5m', '10m', '30m', '1h', '2h', '1h'], 'duration');

    return (
      <div className="u-flex u-jc-space-around u-ai-center">
        Value is
        <Dropdown items={operators} selected={operator} onChoose={this.handleDropdownChange} />
        <input ref={(r) => this.valueInput = r} defaultValue={value} onKeyUp={this.handleInputChange}></input>
        <Dropdown items={relations} selected={relation} onChoose={this.handleDropdownChange} />
        <input ref={(r) => this.percentileInput = r} defaultValue={percentile} onKeyUp={this.handleInputChange}></input>
        during the last
        <Dropdown items={durations} selected={duration} onChoose={this.handleDropdownChange} />
      </div>
    );
  },
});

const Relative = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        func: PropTypes.string,
        change: PropTypes.string,
        duration: PropTypes.string,
        compareDuration: PropTypes.string,
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
    const {func, change, duration, compareDuration, operator, value} = this.props.rule.values;

    function mapToItems(arr, type) {
      return arr.map((text) => {
        return {text, type};
      });
    }

    const funcs = mapToItems(['greater than', 'less than', 'equal to', 'not equal to'], 'func');
    const changes = mapToItems(['change', '% change'], 'change');
    const durations = mapToItems(['1m', '5m', '10m', '30m', '1h', '2h', '1h'], 'duration');
    const compareDurations = mapToItems(['1m', '5m', '10m', '30m', '1h', '2h', '1h'], 'compareDuration');
    const operators = mapToItems(['greater than', 'less than', 'equal to', 'not equal to'], 'operator');

    return (
      <div className="u-flex u-jc-space-around u-ai-center">
        The
        <Dropdown items={funcs} selected={func} onChoose={this.handleDropdownChange} />
        of the
        <Dropdown items={changes} selected={change} onChoose={this.handleDropdownChange} />
        over
        <Dropdown items={durations} selected={duration} onChoose={this.handleDropdownChange} />
        compared to
        <Dropdown items={compareDurations} selected={compareDuration} onChoose={this.handleDropdownChange} />
        before is
        <Dropdown items={operators} selected={operator} onChoose={this.handleDropdownChange} />
        <input ref={(r) => this.input = r} defaultValue={value} onKeyUp={this.handleInputChange}></input>%
      </div>
    );
  },
});

const Deadman = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      values: PropTypes.shape({
        duration: PropTypes.string,
      }),
    }),
    onChange: PropTypes.func.isRequired,
  },

  handleChange(item) {
    this.props.onChange({duration: item.text});
  },

  render() {
    const durations = [{text: '1m'}, {text: '5m'}, {text: '10m'}, {text: '30m'}, {text: '1h'}, {text: '2h'}, {text: '1h'}];

    return (
      <div className="u-flex u-ai-center">
        Create an alert if data is missing for
        <Dropdown items={durations} selected={this.props.rule.values.duration} onChoose={this.handleChange} />
      </div>
    );
  },
});

export default ValuesSection;

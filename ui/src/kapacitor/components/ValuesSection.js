import React, {PropTypes} from 'react';
import Dropdown from 'src/shared/components/Dropdown';
import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs';

const TABS = ['Threshold', 'Relative', 'Deadman'];
export const ValuesSection = React.createClass({
  propTypes: {
    rule: PropTypes.shape({}).isRequired,
  },

  render() {
    return (
      <div className="container-fluid">
        <div className="row">
          <div className="col-md-12">
            <Tabs onSelect={this.handleActivateTab}>
              <TabList>
                {TABS.map(tab => <Tab key={tab}>{tab}</Tab>)}
              </TabList>

              <TabPanels>
                <TabPanel>
                  <Threshold />
                </TabPanel>
                <TabPanel>
                  <Relative />
                </TabPanel>
                <TabPanel>
                  <Deadman />
                </TabPanel>
              </TabPanels>
            </Tabs>
          </div>
        </div>
      </div>
    );
  },
});

const Threshold = React.createClass({
  getInitialState() {
    return {
      operator: 'Greater than',
      value: null,
      relation: 'more than',
      percentile: null,
      duration: '1m',
    };
  },

  handleSelectFromDropdown(stateKey, item) {
    this.setState({[stateKey]: item.text});
  },

  handleTypeValue(e) {
    this.setState({value: e.target.value});
  },

  render() {
    const operators = [{text: 'equal to or greater'}, {text: 'greater than'}, {text: 'less than'}, {text: 'equal to'}, {text: 'not equal to'}];
    const relations = [{text: 'once'}, {text: 'more than '}, {text: 'less than'}];
    const durations = [{text: '1m'}, {text: '5m'}, {text: '10m'}, {text: '30m'}, {text: '1h'}, {text: '2h'}, {text: '1h'}];

    return (
      <div className="u-flex u-jc-space-around u-ai-center">
        Value is
        <Dropdown items={operators} selected={this.state.operator} onChoose={(i) => this.handleSelectFromDropdown('operator', i)} />
        <input placeholder="90" onKeyUp={this.handleTypeValue}></input>
        <Dropdown items={relations} selected={this.state.relation} onChoose={(i) => this.handleSelectFromDropdown('relation', i)} />
        <input placeholder="50%" onKeyUp={this.handleTypePercentile}></input>
        during the last
        <Dropdown items={durations} selected={this.state.duration} onChoose={(i) => this.handleSelectFromDropdown('duration', i)} />
      </div>
    );
  },
});

const Relative = React.createClass({
  getInitialState() {
    return {
      func: 'average',
      change: 'change',
      duration: '1m',
      compareDuration: '1m',
      operator: 'greater than',
      value: null,
    };
  },

  handleSelectFromDropdown(stateKey, item) {
    this.setState({[stateKey]: item.text});
  },

  handleTypePercentile(e) {
    this.setState({percentile: e.target.value});
  },

  render() {
    const funcs = [{text: 'greater than'}, {text: 'less than'}, {text: 'equal to'}, {text: 'not equal to'}];
    const changes = [{text: 'change'}, {text: '% change'}];
    const durations = [{text: '1m'}, {text: '5m'}, {text: '10m'}, {text: '30m'}, {text: '1h'}, {text: '2h'}, {text: '1h'}];
    const compareDurations = [{text: '1m'}, {text: '5m'}, {text: '10m'}, {text: '30m'}, {text: '1h'}, {text: '2h'}, {text: '1h'}];
    const operators = [{text: 'greater than'}, {text: 'less than'}, {text: 'equal to'}, {text: 'not equal to'}];

    return (
      <div className="u-flex u-jc-space-around u-ai-center">
        The
        <Dropdown items={funcs} selected={this.state.func} onChoose={(i) => this.handleSelectFromDropdown('func', i)} />
        of the
        <Dropdown items={changes} selected={this.state.change} onChoose={(i) => this.handleSelectFromDropdown('change', i)} />
        over
        <Dropdown items={durations} selected={this.state.duration} onChoose={(i) => this.handleSelectFromDropdown('duration', i)} />
        compared to
        <Dropdown items={compareDurations} selected={this.state.compareDuration} onChoose={(i) => this.handleSelectFromDropdown('compareDuration', i)} />
        before is
        <Dropdown items={operators} selected={this.state.operator} onChoose={(i) => this.handleSelectFromDropdown('operator', i)} />
        <input placeholder="50" onKeyUp={this.handleTypePercentile}></input>%
      </div>
    );
  },
});

const Deadman = React.createClass({
  getInitialState() {
    return {
      duration: '1m',
    };
  },

  handleSelectDuration(item) {
    this.setState({duration: item.text});
  },

  render() {
    const durations = [{text: '1m'}, {text: '5m'}, {text: '10m'}, {text: '30m'}, {text: '1h'}, {text: '2h'}, {text: '1h'}];

    return (
      <div className="u-flex u-ai-center">
        Create an alert if data is missing for
        <Dropdown items={durations} selected={this.state.duration} onChoose={this.handleSelectDuration} />
      </div>
    );
  },
});

export default ValuesSection;

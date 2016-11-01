import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import DataSection from '../components/DataSection';
import defaultQueryConfig from 'src/utils/defaultQueryConfig';
import * as actionCreators from '../actions/view';
import {bindActionCreators} from 'redux'

const TASK_ID = 1; // switch to this.props.params.taskID

export const KapacitorRulePage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        self: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func,
    tasks: PropTypes.shape({}).isRequired,
  },

  componentDidMount() {
    this.props.actions.fetchTask(TASK_ID);
  },

  render() {
    return (
      <div className="kapacitor-rule-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Kapacitor Rules</h1>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              {this.renderDataSection()}
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              {this.renderValuesSection()}
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              {this.renderMessageSection()}
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              {this.renderAlertsSection()}
            </div>
          </div>
        </div>
      </div>
    );
  },

  renderDataSection() {
    const task = this.props.tasks[Object.keys(this.props.tasks)[0]]; // this.props.params.taskID
    const query = (task && task.query) || defaultQueryConfig('');
    return (
      <div className="kapacitor-rule-section">
        <h3>Data</h3>
        <DataSection source={this.props.source} query={query} />
      </div>
    );
  },

  renderValuesSection() {
    return (
      <div className="kapacitor-rule-section">
        <h3>Values</h3>
      </div>
    );
  },

  renderMessageSection() {
    return (
      <div className="kapacitor-rule-section">
        <h3>Message</h3>
        <textarea />
      </div>
    );
  },

  renderAlertsSection() {
    // hit kapacitor config endpoint and filter sections by the "enabled" property
    const alertOptions = ['Slack', 'VictorOps'].map((destination) => {
      return <option key={destination}>send to {destination}</option>;
    });
    return (
      <div className="kapacitor-rule-section">
        <h3>Alerts</h3>
        <p>The Alert should <select>{alertOptions}</select></p>
      </div>
    );
  },
});

function mapStateToProps(state) {
  return {
    tasks: state.tasks
  };
}

function mapDispatchToProps(dispatch) {
  return {actions: bindActionCreators(actionCreators, dispatch)}
}

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulePage);

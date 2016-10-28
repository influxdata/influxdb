import React, {PropTypes} from 'react';
import DataSection from '../components/DataSection';

const DATA_SECTION = 'data';
const VALUES_SECTION = 'values';
const MESSAGE_SECTION = 'message';
const ALERTS_SECTION = 'alerts';

export const KapacitorRulePage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        self: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      activeSection: DATA_SECTION,
    };
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
    return (
      <div className="kapacitor-rule-section">
        <h3>Data</h3>
        <DataSection source={this.props.source} />
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
    })
    return (
      <div className="kapacitor-rule-section">
        <h3>Alerts</h3>
        <p>The Alert should <select>{alertOptions}</select></p>
      </div>
    );
  },
});

export default KapacitorRulePage;

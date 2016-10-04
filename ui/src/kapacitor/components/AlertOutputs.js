import React, {PropTypes} from 'react';
import {getKapacitorConfig, updateKapacitorConfigSection, testAlertOutput} from 'shared/apis';
import SlackConfig from './SlackConfig';
import SMTPConfig from './SMTPConfig';

const AlertOutputs = React.createClass({
  propTypes: {
    validKapacitor: PropTypes.bool,
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      selectedEndpoint: 'smtp',
      smtpConfig: null,
      slackConfig: null,
    };
  },

  componentDidMount() {
    getKapacitorConfig(this.props.source.id).then(({data: {smtp, slack}}) => {
      this.setState({
        slackConfig: (slack && slack.length && slack[0]) || null,
        smtpConfig: (smtp && smtp.length && smtp[0]) || null,
      });
    });
  },

  handleSaveConfig(section, properties) {
    console.log("going to update");
    if (section !== '') {
      console.log("doing it");
      updateKapacitorConfigSection(this.props.source.id, section, Object.assign({}, properties, {enabled: true})).then(() => {
        // slack test can happen
      });      
    }
  },

  changeSelectedEndpoint(e) {
    console.log("change: ", e);
    this.setState({
      selectedEndpoint: e.target.value,
    });
  },

  testSlack(e) {
    e.preventDefault();
    console.log("testing slack");
    testAlertOutput(this.props.source.id, 'slack')
  },

  render() {
    if (!this.props.validKapacitor) {
      return (
        <div className="panel-body">
          Set your Kapacitor connection info to configure alerting endpoints.
        </div>
      );
    } else {
      return (
        <div className="panel-body">
          <h4 className="text-center">Alert Endpoints</h4>
          <br/>
          <div className="row">
            <div className="form-group col-xs-7 col-sm-5 col-sm-offset-2">
              <label htmlFor="alert-endpoint" className="sr-only">Alert Enpoint</label>
              <select className="form-control" id="source" onChange={this.changeSelectedEndpoint}>
                <option key='smtp' value='smtp'>SMTP</option>;
                <option key='slack' value='slack'>Slack</option>;
              </select>
            </div>

            {this.renderAlertConfig(this.state.selectedEndpoint)}

          </div>
        </div>
      );
    }
  },

  renderAlertConfig(endpoint) {
    const save = (properties) => {
      this.handleSaveConfig(endpoint, properties);
    };

    if (endpoint === 'smtp' && this.state.smtpConfig) {
      return <SMTPConfig onSave={save} config={this.state.smtpConfig} />;
    }
    if (endpoint === 'slack' && this.state.slackConfig) {
      return <SlackConfig onSave={save} onTest={this.testSlack} config={this.state.slackConfig} />;
    } 

    return (
      <div>Ima spinner</div>
    );
  },
});

export default AlertOutputs;
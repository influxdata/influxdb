import React, {PropTypes} from 'react';
import {getKapacitorConfig, updateKapacitorConfigSection, testAlertOutput} from 'shared/apis';
import SlackConfig from './SlackConfig';
import SMTPConfig from './SMTPConfig';

const AlertOutputs = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }).isRequired,
    kapacitor: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }),
  },

  getInitialState() {
    return {
      selectedEndpoint: 'smtp',
      smtpConfig: null,
      slackConfig: null,
    };
  },

  componentDidMount() {
    getKapacitorConfig(this.props.kapacitor).then(({data: {smtp, slack}}) => {
      this.setState({
        slackConfig: (slack.elements.length && slack.elements[0]) || null,
        smtpConfig: (smtp.elements.length && smtp.elements[0]) || null,
      });
    });
  },

  handleSaveConfig(section, properties) {
    if (section !== '') {
      updateKapacitorConfigSection(this.props.kapacitor, section, Object.assign({}, properties, {enabled: true})).then(() => {
        // slack test can happen
      });
    }
  },

  changeSelectedEndpoint(e) {
    this.setState({
      selectedEndpoint: e.target.value,
    });
  },

  testSlack(e) {
    e.preventDefault();
    testAlertOutput(this.props.kapacitor, 'slack');
  },

  render() {
    return (
      <div className="panel-body">
        <h4 className="text-center">Alert Endpoints</h4>
        <br/>
        <div className="row">
          <div className="form-group col-xs-7 col-sm-5 col-sm-offset-2">
            <label htmlFor="alert-endpoint" className="sr-only">Alert Enpoint</label>
            <select className="form-control" id="source" onChange={this.changeSelectedEndpoint}>
              <option key="smtp" value="smtp">SMTP</option>;
              <option key="slack" value="slack">Slack</option>;
            </select>
          </div>
        </div>
        <div className="row">
          {this.renderAlertConfig(this.state.selectedEndpoint)}
        </div>
      </div>
    );
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

    return <div>This endpoint is not supported yet!</div>;
  },
});

export default AlertOutputs;

import React, {PropTypes} from 'react';
import _ from 'lodash';
import {getKapacitorConfig, updateKapacitorConfigSection, testAlertOutput} from 'shared/apis';
import SlackConfig from './SlackConfig';
import SMTPConfig from './SMTPConfig';
import VictoropsConfig from './VictoropsConfig';
import AlertaConfig from './AlertaConfig';

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
    addFlashMessage: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      selectedEndpoint: 'alerta',
      smtpConfig: null,
      slackConfig: null,
      alertaConfig: null,
    };
  },

  componentDidMount() {
    this.refreshKapacitorConfig();
  },

  refreshKapacitorConfig() {
    getKapacitorConfig(this.props.kapacitor).then(({data: {sections}}) => {
      this.setState({
        alertaConfig: _.get(sections, ['alerta', 'elements', '0'], null),
        slackConfig: _.get(sections, ['slack', 'elements', '0'], null),
        smtpConfig: _.get(sections, ['smtp', 'elements', '0'], null),
        victoropsConfig: _.get(sections, ['victorops', 'elements', '0'], null),
      });
    });
  },

  handleSaveConfig(section, properties) {
    if (section !== '') {
      updateKapacitorConfigSection(this.props.kapacitor, section, Object.assign({}, properties, {enabled: true})).then(() => {
        this.refreshKapacitorConfig();
        this.props.addFlashMessage({
          type: 'success',
          text: `Alert for ${section} successfully saved`,
        });
      }).catch(() => {
        this.props.addFlashMessage({
          type: 'error',
          text: `There was an error saving the kapacitor config`,
        });
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
    testAlertOutput(this.props.kapacitor, 'slack').then(() => {
      this.props.addFlashMessage({type: 'success', text: 'Slack test message sent'});
    }).catch(() => {
      this.props.addFlashMessage({type: 'error', text: `There was an error testing the slack alert`});
    });
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
              <option value="alerta">Alerta</option>
              <option value="slack">Slack</option>
              <option value="smtp">SMTP</option>
              <option value="victorops">VictorOps</option>
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

    if (endpoint === 'alerta' && this.state.alertaConfig) {
      return <AlertaConfig onSave={save} config={this.state.alertaConfig} />;
    }

    if (endpoint === 'smtp' && this.state.smtpConfig) {
      return <SMTPConfig onSave={save} config={this.state.smtpConfig} />;
    }

    if (endpoint === 'slack' && this.state.slackConfig) {
      return <SlackConfig onSave={save} onTest={this.testSlack} config={this.state.slackConfig} />;
    }

    if (endpoint === 'victorops' && this.state.victoropsConfig) {
      return <VictoropsConfig onSave={save} config={this.state.victoropsConfig} />;
    }

    return <div>This endpoint is not supported yet!</div>;
  },
});

export default AlertOutputs;

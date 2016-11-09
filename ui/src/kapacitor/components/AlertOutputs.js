import React, {PropTypes} from 'react';
import _ from 'lodash';
import {getKapacitorConfig, updateKapacitorConfigSection, testAlertOutput} from 'shared/apis';
import AlertaConfig from './AlertaConfig';
import HipchatConfig from './HipchatConfig';
import PagerdutyConfig from './PagerdutyConfig';
import SensuConfig from './SensuConfig';
import SlackConfig from './SlackConfig';
import SMTPConfig from './SMTPConfig';
import TelegramConfig from './TelegramConfig';
import VictoropsConfig from './VictoropsConfig';

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
      selectedEndpoint: 'smtp',
      alertaConfig: null,
      smtpConfig: null,
      slackConfig: null,
      telegramConfig: null,
      hipchatConfig: null,
      sensuConfig: null,
      canConnect: false,
    };
  },

  componentDidMount() {
    this.refreshKapacitorConfig();
  },

  refreshKapacitorConfig() {
    getKapacitorConfig(this.props.kapacitor).then(({data: {sections}}) => {
      this.setState({
        alertaConfig: this.getSection(sections, 'alerta'),
        hipchatConfig: this.getSection(sections, 'hipchat'),
        pagerdutyConfig: this.getSection(sections, 'pagerduty'),
        slackConfig: this.getSection(sections, 'slack'),
        smtpConfig: this.getSection(sections, 'smtp'),
        telegramConfig: this.getSection(sections, 'telegram'),
        victoropsConfig: this.getSection(sections, 'victorops'),
        sensuConfig: this.getSection(sections, 'sensu'),
      });
      this.setState({canConnect: true});
    }).catch(() => {
      this.setState({canConnect: false});
    });
  },

  getSection(sections, section) {
    return _.get(sections, [section, 'elements', '0'], null);
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
        { this.renderBody() }
      </div>
    );
  },

  renderBody() {
    let body;
    if (this.state.canConnect) {
      body = (
        <div>
          <div className="row">
            <div className="form-group col-xs-7 col-sm-5 col-sm-offset-2">
              <label htmlFor="alert-endpoint" className="sr-only">Alert Enpoint</label>
              <select value={this.state.selectedEndpoint} className="form-control" id="source" onChange={this.changeSelectedEndpoint}>
                <option value="hipchat">HipChat</option>
                <option value="pagerduty">PagerDuty</option>
                <option value="sensu">Sensu</option>
                <option value="slack">Slack</option>
                <option value="smtp">SMTP</option>
                <option value="telegram">Telegram</option>
                <option value="victorops">VictorOps</option>
              </select>
            </div>
          </div>
          <div className="row">
            {this.renderAlertConfig(this.state.selectedEndpoint)}
          </div>
        </div>
      );
    } else {
      body = (<p className="error">Cannot connect.</p>);
    }
    return body;
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

    if (endpoint === 'telegram' && this.state.telegramConfig) {
      return <TelegramConfig onSave={save} config={this.state.telegramConfig} />;
    }

    if (endpoint === 'pagerduty' && this.state.pagerdutyConfig) {
      return <PagerdutyConfig onSave={save} config={this.state.pagerdutyConfig} />;
    }

    if (endpoint === 'hipchat' && this.state.hipchatConfig) {
      return <HipchatConfig onSave={save} config={this.state.hipchatConfig} />;
    }

    if (endpoint === 'sensu' && this.state.sensuConfig) {
      return <SensuConfig onSave={save} config={this.state.sensuConfig} />;
    }

    return <div></div>;
  },
});

export default AlertOutputs;

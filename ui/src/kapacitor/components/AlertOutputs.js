import React, {PropTypes} from 'react';
import _ from 'lodash';
import {getKapacitorConfig, updateKapacitorConfigSection, testAlertOutput} from 'shared/apis';
import AlertaConfig from './AlertaConfig';
import HipChatConfig from './HipChatConfig';
import OpsGenieConfig from './OpsGenieConfig';
import PagerDutyConfig from './PagerDutyConfig';
import SensuConfig from './SensuConfig';
import SlackConfig from './SlackConfig';
import SMTPConfig from './SMTPConfig';
import TalkConfig from './TalkConfig';
import TelegramConfig from './TelegramConfig';
import VictorOpsConfig from './VictorOpsConfig';

const AlertOutputs = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }).isRequired,
    kapacitor: PropTypes.shape({
      url: PropTypes.string.isRequired,
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      selectedEndpoint: 'smtp',
      configSections: null,
    };
  },

  componentDidMount() {
    this.refreshKapacitorConfig(this.props.kapacitor);
  },

  componentWillReceiveProps(nextProps) {
    if (this.props.kapacitor.url !== nextProps.kapacitor.url) {
      this.refreshKapacitorConfig(nextProps.kapacitor);
    }
  },

  refreshKapacitorConfig(kapacitor) {
    getKapacitorConfig(kapacitor).then(({data: {sections}}) => {
      this.setState({configSections: sections});
    }).catch(() => {
      this.setState({configSections: null});
      this.props.addFlashMessage({type: 'error', text: `There was an error getting the Kapacitor config`});
    });
  },

  getSection(sections, section) {
    return _.get(sections, [section, 'elements', '0'], null);
  },

  handleSaveConfig(section, properties) {
    if (section !== '') {
      const propsToSend = this.sanitizeProperties(section, properties);
      updateKapacitorConfigSection(this.props.kapacitor, section, propsToSend).then(() => {
        this.refreshKapacitorConfig(this.props.kapacitor);
        this.props.addFlashMessage({type: 'success', text: `Alert for ${section} successfully saved`});
      }).catch(() => {
        this.props.addFlashMessage({type: 'error', text: `There was an error saving the kapacitor config`});
      });
    }
  },

  changeSelectedEndpoint(e) {
    this.setState({
      selectedEndpoint: e.target.value,
    });
  },

  handleTest(section, properties) {
    const propsToSend = this.sanitizeProperties(section, properties);
    testAlertOutput(this.props.kapacitor, section, propsToSend).then(() => {
      this.props.addFlashMessage({type: 'success', text: 'Slack test message sent'});
    }).catch(() => {
      this.props.addFlashMessage({type: 'error', text: `There was an error testing the slack alert`});
    });
  },

  sanitizeProperties(section, properties) {
    const cleanProps = Object.assign({}, properties, {enabled: true});
    const {redacted} = this.getSection(this.state.configSections, section);
    if (redacted && redacted.length) {
      redacted.forEach((badProp) => {
        if (properties[badProp] === 'true') {
          delete cleanProps[badProp];
        }
      });
    }
    return cleanProps;
  },

  render() {
    const {configSections, selectedEndpoint} = this.state;
    if (!configSections) { // could use this state to conditionally render spinner or error message
      return null;
    }

    return (
      <div className="panel panel-minimal">
        <div className="panel-body">
          <h4 className="text-center">Configure Alert Endpoints</h4>
          <br/>
          <div className="row">
            <div className="form-group col-xs-12 col-sm-8 col-sm-offset-2">
              <label htmlFor="alert-endpoint" className="sr-only">Alert Enpoint</label>
              <select value={this.state.selectedEndpoint} className="form-control" id="source" onChange={this.changeSelectedEndpoint}>
                <option value="alerta">Alerta</option>
                <option value="hipchat">HipChat</option>
                <option value="opsgenie">OpsGenie</option>
                <option value="pagerduty">PagerDuty</option>
                <option value="sensu">Sensu</option>
                <option value="slack">Slack</option>
                <option value="smtp">SMTP</option>
                <option value="talk">Talk</option>
                <option value="telegram">Telegram</option>
                <option value="victorops">VictorOps</option>
              </select>
            </div>
          </div>
          <div className="row">
            <div className="col-xs-12 col-sm-8 col-sm-offset-2">
              <hr/>
            </div>
            <div className="col-xs-12 col-sm-8 col-sm-offset-2">
              {this.renderAlertConfig(selectedEndpoint)}
            </div>
          </div>
        </div>
      </div>
    );
  },

  renderAlertConfig(endpoint) {
    const {configSections} = this.state;
    const save = (properties) => {
      this.handleSaveConfig(endpoint, properties);
    };
    const test = (properties) => {
      this.handleTest(endpoint, properties);
    };

    switch (endpoint) {
      case 'alerta': {
        return <AlertaConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'smtp': {
        return <SMTPConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'slack': {
        return <SlackConfig onSave={save} onTest={test} config={this.getSection(configSections, endpoint)} />;
      }
      case 'victorops': {
        return <VictorOpsConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'telegram': {
        return <TelegramConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'opsgenie': {
        return <OpsGenieConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'pagerduty': {
        return <PagerDutyConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'hipchat': {
        return <HipChatConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'sensu': {
        return <SensuConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
      case 'talk': {
        return <TalkConfig onSave={save} config={this.getSection(configSections, endpoint)} />;
      }
    }
  },
});

export default AlertOutputs;

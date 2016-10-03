import React, {PropTypes} from 'react';
import _ from 'lodash';
import {getKapacitorConfig, updateKapacitorConfigSection, testAlertOutput} from 'shared/apis';

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
    };
  },

  componentDidMount() {
    getKapacitorConfig(this.props.source.id).then((res) => {
      console.log("setting state", res.data);

      if (res.data.smtp && res.data.smtp.length > 0) {
        const smtp = res.data.smtp[0];
        this.setState({
          smtpHost: smtp.host,
          smtpPort: smtp.port,
          smtpUser: smtp.username,
          smtpFrom: smtp.from,
        });
      }

      if (res.data.slack && res.data.slack.length > 0) {
        const slack = res.data.slack[0];
        this.setState({
          slackChannel: slack.channel,
          slackURL: slack.url,
        });
      }
    })
  },

  handleAlertConfig(e) {
    e.preventDefault();

    let section = this.state.selectedEndpoint;
    let properties;

    switch(this.state.selectedEndpoint) {
      case 'smtp':
        properties = {
          enabled: true,
          host: this.state.smtpHost,
          port: this.state.smtpPort,
          from: this.state.smtpFrom,
          username: this.state.smtpUser,
          password: this.state.smtpPassword,
        };
        break;

      case 'slack':
        properties = {
          enabled: true,
          channel: this.state.slackChannel,
          url: this.state.slackURL,
        };
        break;

      default:
        section = '';
        console.log(`${this.state.selectedEndpoint} not implemented!`);
    }

    console.log("going to update");

    if (section != '') {
      console.log("doing it");
      updateKapacitorConfigSection(this.props.source.id, section, properties).then((res) => {
        console.log("updating: ", section, properties);
      });      
    }
  },

  changeSelectedEndpoint(e) {
    console.log("change: ", e);
    this.setState({
      selectedEndpoint: e.target.value,
    });
  },

  changeSMTPHost(e) {
    this.setState({
      smtpHost: e.target.value,
    });
  },

  changeSMTPPort(e) {
    this.setState({
      smtpPort: e.target.value,
    });
  },

  changeSMTPFrom(e) {
    this.setState({
      smtpFrom: e.target.value,
    });
  },

  changeSMTPUser(e) {
    this.setState({
      smtpUser: e.target.value,
    });
  },

  changeSMTPPassword(e) {
    this.setState({
      smtpPassword: e.target.value,
    });
  },

  changeSlackURL(e) {
    this.setState({
      slackURL: e.target.value,
    });
  },

  changeSlackChannel(e) {
    this.setState({
      slackChannel: e.target.value,
    });
  },

  testSlack(e) {
    e.preventDefault();
    console.log("testing slack");
    testAlertOutput(this.props.source.id, 'slack')
  },

  alertEndpointClass(endpointName) {
    if (endpointName === this.state.selectedEndpoint) {
      return "col-xs-7 col-sm-8 col-sm-offset-2";
    } else {
      return "col-xs-7 col-sm-8 col-sm-offset-2 hidden";
    }
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
          <form onSubmit={this.handleAlertConfig}>
            <div className="row">
              <div className="form-group col-xs-7 col-sm-5 col-sm-offset-2">
                <label htmlFor="alert-endpoint" className="sr-only">Alert Enpoint</label>
                <select className="form-control" id="source" onChange={this.changeSelectedEndpoint}>
                  <option key='smtp' value='smtp'>SMTP</option>;
                  <option key='slack' value='slack'>Slack</option>;
                </select>
              </div>

              <div className={this.alertEndpointClass('smtp')}>
                <p>
                  You can have alerts sent to an email address by setting up an SMTP endpoint.
                </p>

                <div className="form-group">
                  <label htmlFor="smtp-host">SMTP Host</label>
                  <input className="form-control" id="smtp-host" type="text" value={this.state.smtpHost || ''} onChange={this.changeSMTPHost}></input>
                </div>

                <div className="form-group">
                  <label htmlFor="smtp-port">SMTP Port</label>
                  <input className="form-control" id="smtp-port" type="text" value={this.state.smtpPort || ''} onChange={this.changeSMTPPort}></input>
                </div>

                <div className="form-group">
                  <label htmlFor="smtp-from">From email</label>
                  <input className="form-control" id="smtp-from" type="text" value={this.state.smtpFrom || ''} onChange={this.changeSMTPFrom}></input>
                </div>

                <div className="form-group">
                  <label htmlFor="smtp-user">User</label>
                  <input className="form-control" id="smtp-user" type="text" value={this.state.smtpUser || ''} onChange={this.changeSMTPUser}></input>
                </div>

                <div className="form-group">
                  <label htmlFor="smtp-password">Password</label>
                  <input className="form-control" id="smtp-password" type="password" value={this.state.smtpPassword || ''} onChange={this.changeSMTPPassword}></input>
                </div>
              </div>

              <div className={this.alertEndpointClass('slack')}>
                <p>
                  Post alerts to a Slack channel.
                </p>

                <div className="form-group">
                  <label htmlFor="slack-url">Slack Webhook URL (<a href="https://api.slack.com/incoming-webhooks" target="_">see more on Slack webhooks</a>)</label>
                  <input className="form-control" id="slack-url" type="text" value={this.state.slackURL || ''} onChange={this.changeSlackURL}></input>
                </div>

                <div className="form-group">
                  <label htmlFor="slack-channel">Slack Channel (optional)</label>
                  <input className="form-control" id="slack-channel" type="text" placeholder="#alerts" value={this.state.slackChannel || ''} onChange={this.changeSlackChannel}></input>
                </div>

                <a className="btn btn-warning" onClick={this.testSlack} disabled={this.state.slackURL != '' ? false : true}>Test</a>
              </div>
            </div>
            <hr />
            <div className="row">
              <div className="form-group col-xs-5 col-sm-3 col-sm-offset-2">
                <button className="btn btn-block btn-primary" type="submit">Save</button>
              </div>
            </div>
          </form>          
        </div>
      );
    }
  },
});

export default AlertOutputs;
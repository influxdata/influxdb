import React, {PropTypes} from 'react';
import {getKapacitor, createKapacitor, updateKapacitor, pingKapacitor} from 'shared/apis';
import AlertOutputs from '../components/AlertOutputs';

export const KapacitorPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }),
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      kapacitor: null,
      canConnect: false,
    };
  },

  componentDidMount() {
    this.fetchKapacitor();
  },

  fetchKapacitor() {
    const {source} = this.props;
    getKapacitor(source).then((kapacitor) => {
      if (!kapacitor) {
        return;
      }
      this.setState({kapacitor}, () => {
        pingKapacitor(kapacitor).catch(() => {
          this.props.addFlashMessage({type: 'error', text: 'Could not connect to Kapacitor. Check settings.'});
        });
      });
    });
  },

  handleKapacitorUpdate(e) {
    e.preventDefault();
    if (this.state.kapacitor) {
      this.handleUpdateKapacitor();
    } else {
      this.handleCreateKapacitor();
    }
  },

  handleCreateKapacitor() {
    const {source} = this.props;
    const {newURL, newName, newUsername} = this.state;
    createKapacitor(source, {
      url: newURL.trim(),
      name: newName.trim(),
      username: newUsername,
      password: this.kapacitorPassword.value,
    }).then(({data: createdKapacitor}) => {
      this.props.addFlashMessage({type: 'success', text: 'Kapacitor Created!'});
      this.setState({kapacitor: createdKapacitor});
    }).catch(() => {
      this.props.addFlashMessage({type: 'error', text: 'There was a problem creating the Kapacitor record'});
    });
  },

  handleUpdateKapacitor() {
    const {addFlashMessage, source} = this.props;
    const {kapacitor, newURL, newName, newUsername} = this.state;
    updateKapacitor(kapacitor, {
      url: (newURL || kapacitor.url).trim(),
      name: (newName || kapacitor.name).trim(),
      username: newUsername || kapacitor.username,
      password: this.kapacitorPassword.value,
    }).then(({data: newKapacitor}) => {
      pingKapacitor(kapacitor).then(({data: {elements}}) => {
        this.setState({kapacitor: newKapacitor});
        const sourceMatch = elements[0].options.urls.some((url) => url === source.url);
        if (!sourceMatch && kapacitor.url !== newKapacitor.url) {
          addFlashMessage({type: 'warning', text: `Warning: Kapacitor is configured to use an instance of InfluxDB which does not match the URL of your current source. Please ensure your InfluxDB source and Kapacitor's InfluxDB configuration point to the same server.`});
        }
        addFlashMessage({type: 'success', text: 'Kapacitor Saved!'});
      }).catch(() => {
        this.props.addFlashMessage({type: 'error', text: 'Kapacitor Saved, but cannot connect. Check settings.'});
      });
    }).catch(() => {
      addFlashMessage({type: 'error', text: 'There was a problem updating the Kapacitor record'});
    });
  },

  updateName() {
    this.setState({newName: this.kapacitorName.value});
  },

  updateURL() {
    this.setState({newURL: this.kapacitorURL.value});
  },

  updateUsername() {
    this.setState({newUsername: this.kapacitorUser.value});
  },

  render() {
    const {kapacitor, newName, newURL, newUsername} = this.state;
    // if the fields in state are defined, use them. otherwise use the defaults
    const name = newName === undefined ? kapacitor && kapacitor.name || '' : newName;
    const url = newURL === undefined ? kapacitor && kapacitor.url || '' : newURL;
    const username = newUsername === undefined ? kapacitor && kapacitor.username || '' : newUsername;

    return (
      <div className="kapacitor">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Configure Kapacitor
              </h1>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-minimal">
                <div className="panel-body">
                  <p>
                    Kapacitor is used as the monitoring and alerting agent.
                    This page will let you configure which Kapacitor to use and
                    set up alert end points like email, Slack, and others.
                  </p>
                  <hr/>
                  <h4 className="text-center">Connection Details</h4>
                  <br/>
                  <form onSubmit={this.handleKapacitorUpdate}>
                    <div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="connect-string">Connection String</label>
                        <input ref={(r) => this.kapacitorURL = r} className="form-control" id="connect-string" placeholder="http://localhost:9092" value={url} onChange={this.updateURL}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="name">Name</label>
                        <input ref={(r) => this.kapacitorName = r} className="form-control" id="name" placeholder="My Kapacitor" value={name} onChange={this.updateName}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="username">Username</label>
                        <input ref={(r) => this.kapacitorUser = r} className="form-control" id="username" value={username} onChange={this.updateUsername}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="password">Password</label>
                        <input ref={(r) => this.kapacitorPassword = r} className="form-control" id="password" type="password"></input>
                      </div>
                    </div>

                    <div className="form-group col-xs-4 col-xs-offset-4">
                      <button className="btn btn-block btn-success" type="submit">Connect Kapacitor</button>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              {this.renderAlertOutputs()}
            </div>
          </div>
        </div>
      </div>
    );
  },

  renderAlertOutputs() {
    const {kapacitor} = this.state;
    if (kapacitor) {
      return <AlertOutputs source={this.props.source} kapacitor={kapacitor} addFlashMessage={this.props.addFlashMessage} />;
    }

    return (
      <div className="panel-body">
        Set your Kapacitor connection info to configure alerting endpoints.
      </div>
    );
  },
});

export default KapacitorPage;

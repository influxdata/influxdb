import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import FlashMessages from 'shared/components/FlashMessages';
import {createSource, getSources} from 'shared/apis';

export const SelectSourcePage = React.createClass({
  propTypes: {
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
    location: PropTypes.shape({
      query: PropTypes.shape({
        redirectPath: PropTypes.string,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      sources: [],
      selectedSource: undefined,
    };
  },

  componentDidMount() {
    getSources().then(({data: {sources}}) => {
      this.setState({
        sources,
      });
    });
  },

  handleSelectSource(e) {
    e.preventDefault();
    const source = this.state.sources.find((s) => s.name === (this.state.selectedSource || this.sourceSelect.value));
    this.redirectToApp(source);
  },

  handleNewSource(e) {
    e.preventDefault();
    const source = {
      url: this.sourceURL.value,
      name: this.sourceName.value,
      username: this.sourceUser.value,
      password: this.sourcePassword.value,
    };
    createSource(source).then(({data: sourceFromServer}) => {
      this.setState({
        sources: this.state.sources.concat(sourceFromServer),
        selectedSource: sourceFromServer.name,
      });
      this.sourceURL.value = '';
      this.sourceName.value = '';
      this.sourceUser.value = '';
      this.sourcePassword.value = '';
      this.sourceSelect.focus();
    });
  },

  handleChangeSourceSelection() {
    this.setState({
      selectedSource: this.sourceSelect.value,
    });
  },

  redirectToApp(source) {
    const {redirectPath} = this.props.location.query;
    if (!redirectPath) {
      return this.props.router.push(`/sources/${source.id}/hosts`);
    }

    const fixedPath = redirectPath.replace(/\/sources\/[^/]*/, `/sources/${source.id}`);
    return this.props.router.push(fixedPath);
  },

  render() {
    const error = !!this.props.location.query.redirectPath;

    const selectProps = {
      className: 'form-control',
      id: 'source',
      ref: (r) => {
        this.sourceSelect = r;
      },
      onChange: this.handleChangeSourceSelection,
      value: this.state.selectedSource,
    };

    return (
      <div id="select-source-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-heading text-center">
                  <h2 className="deluxe">Welcome to Chronograf</h2>
                </div>
                <div className="panel-body">
                  <br/>
                  <h4 className="text-center">Select an InfluxDB Server to connect to</h4>
                  <br/>
                  <form onSubmit={this.handleSelectSource}>
                    <div className="form-group col-sm-8 col-sm-offset-2">
                      {error ? <div className="alert alert-danger"><span className="icon alert-triangle"></span>Data source not found or unavailable</div> : null}
                    </div>
                    <div className="form-group col-xs-7 col-sm-5 col-sm-offset-2">
                      <label htmlFor="source" className="sr-only">Detected InfluxDB Servers</label>
                      <select {...selectProps}>
                        {this.state.sources.map(({name}) => {
                          return <option key={name} value={name}>{name}</option>;
                        })}
                      </select>
                    </div>

                    <div className="form-group col-xs-5 col-sm-3">
                      <button className="btn btn-block btn-primary" type="submit">Connect</button>
                    </div>
                  </form>

                  <br/>
                  <hr/>
                  <br/>
                  <h4 className="text-center">Or connect to a New Server</h4>
                  <br/>

                  <form onSubmit={this.handleNewSource}>
                    <div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="connect-string">Connection String</label>
                        <input ref={(r) => this.sourceURL = r} className="form-control" id="connect-string" placeholder="http://localhost:8086"></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="name">Name</label>
                        <input ref={(r) => this.sourceName = r} className="form-control" id="name" placeholder="Influx 1"></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="username">Username</label>
                        <input ref={(r) => this.sourceUser = r} className="form-control" id="username"></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="password">Password</label>
                        <input ref={(r) => this.sourcePassword = r} className="form-control" id="password" type="password"></input>
                      </div>
                    </div>

                    <div className="form-group col-xs-12 text-center">
                      <button className="btn btn-success" type="submit">Create New Server</button>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default FlashMessages(withRouter(SelectSourcePage));

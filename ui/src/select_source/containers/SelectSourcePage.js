import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import FlashMessages from 'shared/components/FlashMessages';
import {createSource, getSources} from 'shared/apis';

export const SelectSourcePage = React.createClass({
  propTypes: {
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      sources: [],
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
    const source = this.state.sources.find((s) => s.name === this.selectedSource.value);
    this.props.router.push(`/sources/${source.id}/hosts`);
  },

  handleNewSource(e) {
    e.preventDefault();
    const source = {
      url: this.sourceURL.value,
      name: this.sourceName.value,
      username: this.sourceUser.value,
      password: this.sourcePassword.value,
    };
    createSource(source).then(() => {
      // this.props.router.push(`/sources/${source.id}/hosts`);
    });
  },

  render() {
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
                  <form onSubmit={this.handleSelectSource}>
                    <div className="form-group col-sm-12">
                      <h4>Select an InfluxDB server to connect to</h4>
                      <label htmlFor="source">InfluxDB Server</label>
                      <select className="form-control input-lg" id="source">
                        {this.state.sources.map(({name}) => {
                          return <option ref={(r) => this.selectedSource = r} key={name} value={name}>{name}</option>;
                        })}
                      </select>
                    </div>

                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button className="btn btn-lg btn-block btn-success" type="submit">Connect</button>
                    </div>
                  </form>

                  <form onSubmit={this.handleNewSource}>
                    <div>
                      <h4>Or connect to a new server</h4>
                      <label htmlFor="connect-string">connection string</label>
                      <input ref={(r) => this.sourceURL = r} id="connect-string" placeholder="http://localhost:8086"></input>
                      <label htmlFor="name">name</label>
                      <input ref={(r) => this.sourceName = r} id="name" placeholder="Influx 1"></input>
                      <label htmlFor="username">username</label>
                      <input ref={(r) => this.sourceUser = r} id="username"></input>
                      <label htmlFor="password">password</label>
                      <input ref={(r) => this.sourcePassword = r} id="password" type="password"></input>
                    </div>

                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button className="btn btn-lg btn-block btn-success" type="submit">Create</button>
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

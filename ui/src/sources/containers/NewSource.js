import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSource, createSource} from 'shared/apis';

// TODO: Add default checkbox
// TODO: wire up default checkbox
// TODO: make Kapacitor a dropdown
// TODO: populate Kapacitor dropdown
// TODO: make this go back to the manage sources page.
// TODO: repurpose for edit page
// TODO: when editing, prepopulate
//        what part of the lifecycle should edit's be loaded in?
// TODO: loading spinner while waiting for edit page to load.
// TODO: change to SourceForm

export const NewSource = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      id: PropTypes.string,
    }),
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
    return (
    {
        source: {
          url: '',
          name: '',
          username: '',
          password: '',
          isDefault: false,
        },
      }
    );
  },

  componentDidMount() {
    getSource(this.props.params.id).then(({data: source}) => {
      const {url, name, username, password} = source;
      const isDefault = source.default; // default is a reserved word
      const loadedSource = {
        url: url, 
        name: name,
        username: username,
        password: password,
        isDefault: isDefault,
      };
      this.setState({source: loadedSource});
    });
  },

  handleNewSource(e) {
    e.preventDefault();
    const source = {
      url: this.sourceURL.value,
      name: this.sourceName.value,
      username: this.sourceUser.value,
      password: this.sourcePassword.value,
      isDefault: true,
    };
    createSource(source).then(({data: sourceFromServer}) => {
      this.redirectToApp(sourceFromServer);
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

  /*
   * `e` is the chang event
   *
   * Assumes the input is named source[propertyName]
   */
  onInputChange(e) {
    console.log(e.target);
    console.log(e.target.id);
    console.log(e.target.name);
    console.log(e.target.value);
    const val = e.target.value;
    const name = e.target.name;
    const matchResults = name.match(/source\[(\w+)\]/);
    if (matchResults !== null) { 
      const newState = {};
      newState[matchResults[1]] = val;
      console.log(newState);
      this.setState({source: newState});
    }
  },


  render() {
    const source = this.state.source || {};
    return (
      <div id="select-source-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-body">
                  <h4 className="text-center">Connect to a New Server</h4>
                  <br/>

                  <form onSubmit={this.handleNewSource}>
                    <div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="connect-string">Connection String</label>
                        <input type="text" name="source[url]" ref={(r) => this.sourceURL = r} className="form-control" id="connect-string" placeholder="http://localhost:8086" onChange={this.onInputChange} value={this.state.source.url}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="name">Name</label>
                        <input type="text" name="source[name]" ref={(r) => this.sourceName = r} className="form-control" id="name" placeholder="Influx 1" onChange={this.onInputChange} value={this.state.source.name}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="username">Username</label>
                        <input type="text" name="source[username]" ref={(r) => this.sourceUser = r} className="form-control" id="username" onChange={this.onInputChange} value={this.state.source.username}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="password">Password</label>
                        <input type="password" name="source[password]" className="form-control" id="password" onChange={this.onInputChange} value={this.state.source.password}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="database">Database</label>
                        <input type="text" name="source[telegraf]" ref={(r) => this.sourceDatabase = r} className="form-control" id="database" placeholder="telegraf" onChange={this.onInputChange} value={this.state.source.database}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="kapacitor">Kapacitor</label>
                        <select name="source[kapacitor]" ref={(r) => this.sourceKapacitor = r} className="form-control" id="kapacitor">
                          <option>Foo</option>
                          <option>Bar</option>
                          <option>Baz</option>
                        </select>
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
export default withRouter(NewSource);

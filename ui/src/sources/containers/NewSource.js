import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSource, createSource, updateSource} from 'shared/apis';

// TODO: Add default checkbox
// TODO: wire up default checkbox

// TODO: make this go back to the manage sources page.
// TODO: change to SourceForm

// TODO: loading spinner while waiting for edit page to load.
// TODO: make Kapacitor a dropdown
// TODO: populate Kapacitor dropdown

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
      url: '',
      name: '',
      username: '',
      password: '',
      isDefault: false,
      editMode: (this.props.params.id !== undefined),
    });
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
      this.setState(loadedSource);
    });
  },

  handleSubmit(e) {
    e.preventDefault();
    const source = {
      url: this.sourceURL.value,
      name: this.sourceName.value,
      username: this.sourceUsername.value,
      password: this.sourcePassword.value,
      isDefault: true,
    };
    if (this.state.editMode) {
      updateSource(this.props.params.id, source).then(({data: sourceFromServer}) => {
        this.redirectToApp(sourceFromServer);
      });
    } else {
      createSource(source).then(({data: sourceFromServer}) => {
        this.redirectToApp(sourceFromServer);
      });
    }
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
   * `e` is the change event
   *
   * Assumes the input is named source[propertyName]
   */
  onInputChange(e) {
    const val = e.target.value;
    const name = e.target.name;
    const matchResults = name.match(/source\[(\w+)\]/);
    if (matchResults !== null) {
      const newState = {source: {}};
      newState[matchResults[1]] = val;
      this.setState(newState);
    }
  },

  submitBtnLabel() {
    return this.state.editMode ? "Update" : "Create";
  },

  titleText() {
    return this.state.editMode ? "Update Existing Source" : "Connect to a New Source";
  },

  render() {
    // TODO: Replace text above form when Editing
    return (
      <div id="select-source-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-body">
                  <h4 className="text-center">{this.titleText()}</h4>
                  <br/>

                  <form onSubmit={this.handleSubmit}>
                    <div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="connect-string">Connection String</label>
                        <input type="text" name="source[url]" ref={(r) => this.sourceURL = r} className="form-control" id="connect-string" placeholder="http://localhost:8086" onChange={this.onInputChange} value={this.state.url}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="name">Name</label>
                        <input type="text" name="source[name]" ref={(r) => this.sourceName = r} className="form-control" id="name" placeholder="Influx 1" onChange={this.onInputChange} value={this.state.name}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="username">Username</label>
                        <input type="text" name="source[username]" ref={(r) => this.sourceUsername = r} className="form-control" id="username" onChange={this.onInputChange} value={this.state.username}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="password">Password</label>
                        <input type="password" name="source[password]" ref={(r) => this.sourcePassword = r} className="form-control" id="password" onChange={this.onInputChange} value={this.state.password}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="database">Database</label>
                        <input type="text" name="source[telegraf]" ref={(r) => this.sourceDatabase = r} className="form-control" id="database" placeholder="telegraf" onChange={this.onInputChange} value={this.state.database}></input>
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
                      <button className="btn btn-success" type="submit">{this.submitBtnLabel()}</button>
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

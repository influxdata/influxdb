import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSource, createSource, updateSource} from 'shared/apis';
import classNames from 'classnames';

export const SourceForm = React.createClass({
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
    addFlashMessage: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      source: {},
      editMode: this.props.params.id !== undefined,
    };
  },

  componentDidMount() {
    if (!this.state.editMode) {
      return;
    }
    getSource(this.props.params.id).then(({data: source}) => {
      this.setState({source});
    });
  },

  handleSubmit(e) {
    e.preventDefault();
    const {router, params, addFlashMessage} = this.props;
    const newSource = Object.assign({}, this.state.source, {
      url: this.sourceURL.value,
      name: this.sourceName.value,
      username: this.sourceUsername.value,
      password: this.sourcePassword.value,
      'default': this.sourceDefault.checked,
    });
    if (this.state.editMode) {
      updateSource(newSource).then(() => {
        router.push(`/sources/${params.sourceID}/manage-sources`);
        addFlashMessage({type: 'success', text: 'The source was successfully updated'});
      });
    } else {
      createSource(newSource).then(() => {
        router.push(`/sources/${params.sourceID}/manage-sources`);
        addFlashMessage({type: 'success', text: 'The source was successfully created'});
      });
    }
  },

  onInputChange(e) {
    const val = e.target.value;
    const name = e.target.name;
    this.setState((prevState) => {
      const newSource = Object.assign({}, prevState.source, {
        [name]: val,
      });
      return Object.assign({}, prevState, {source: newSource});
    });
  },

  render() {
    const {source, editMode} = this.state;

    if (editMode && !source.id) {
      return <div className="page-spinner"></div>;
    }

    return (
      <div id="source-form-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                {editMode ? "Edit Source" : "Add a New Source"}
              </h1>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-body">
                  <h4 className="text-center">Connection Details</h4>
                  <br/>

                  <form onSubmit={this.handleSubmit}>
                    <div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="connect-string">Connection String</label>
                        <input type="text" name="url" ref={(r) => this.sourceURL = r} className="form-control" id="connect-string" placeholder="http://localhost:8086" onChange={this.onInputChange} value={source.url || ''}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="name">Name</label>
                        <input type="text" name="name" ref={(r) => this.sourceName = r} className="form-control" id="name" placeholder="Influx 1" onChange={this.onInputChange} value={source.name || ''}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
                        <label htmlFor="username">Username</label>
                        <input type="text" name="username" ref={(r) => this.sourceUsername = r} className="form-control" id="username" onChange={this.onInputChange} value={source.username || ''}></input>
                      </div>
                      <div className="form-group col-xs-6 col-sm-4">
                        <label htmlFor="password">Password</label>
                        <input type="password" name="password" ref={(r) => this.sourcePassword = r} className="form-control" id="password" onChange={this.onInputChange} value={source.password || ''}></input>
                      </div>
                      <div className="form-group col-xs-8 col-xs-offset-2">
                        <div className="form-control-static">
                          <input type="checkbox" id="defaultSourceCheckbox" defaultChecked={source.default} ref={(r) => this.sourceDefault = r} />
                          <label htmlFor="defaultSourceCheckbox">Make this the default source</label>
                        </div>
                      </div>
                    </div>

                    <div className="form-group col-xs-4 col-xs-offset-4">
                      <button className={classNames('btn btn-block', {'btn-primary': editMode, 'btn-success': !editMode})} type="submit">{editMode ? "Save Changes" : "Add Source"}</button>
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
export default withRouter(SourceForm);

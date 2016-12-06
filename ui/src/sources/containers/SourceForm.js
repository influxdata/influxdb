import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSource, createSource, updateSource} from 'shared/apis';
import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
} from 'shared/actions/sources';
import classNames from 'classnames';
import {connect} from 'react-redux';

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
    addSourceAction: PropTypes.func,
    updateSourceAction: PropTypes.func,
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
      url: this.sourceURL.value.trim(),
      name: this.sourceName.value,
      username: this.sourceUsername.value,
      password: this.sourcePassword.value,
      'default': this.sourceDefault.checked,
      telegraf: this.sourceTelegraf.value,
    });
    if (this.state.editMode) {
      updateSource(newSource).then(({data: sourceFromServer}) => {
        this.props.updateSourceAction(sourceFromServer);
        router.push(`/sources/${params.sourceID}/manage-sources`);
        addFlashMessage({type: 'success', text: 'The source was successfully updated'});
      }).catch(() => {
        addFlashMessage({type: 'error', text: 'There was a problem updating the source. Check the settings'});
      });
    } else {
      createSource(newSource).then(({data: sourceFromServer}) => {
        this.props.addSourceAction(sourceFromServer);
        router.push(`/sources/${params.sourceID}/manage-sources`);
        addFlashMessage({type: 'success', text: 'The source was successfully created'});
      }).catch(() => {
        addFlashMessage({type: 'error', text: 'There was a problem creating the source. Check the settings'});
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
      <div className="page" id="source-form-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                {editMode ? "Edit Source" : "Add a New Source"}
              </h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-8 col-md-offset-2">
                <div className="panel panel-minimal">
                  <div className="panel-body">
                    <h4 className="text-center">Connection Details</h4>
                    <br/>

                    <form onSubmit={this.handleSubmit}>
                      <div className="form-group col-xs-12 col-sm-6">
                        <label htmlFor="connect-string">Connection String</label>
                        <input type="text" name="url" ref={(r) => this.sourceURL = r} className="form-control" id="connect-string" placeholder="http://localhost:8086" onChange={this.onInputChange} value={source.url || ''}></input>
                      </div>
                      <div className="form-group col-xs-12 col-sm-6">
                        <label htmlFor="name">Name</label>
                        <input type="text" name="name" ref={(r) => this.sourceName = r} className="form-control" id="name" placeholder="Influx 1" onChange={this.onInputChange} value={source.name || ''}></input>
                      </div>
                      <div className="form-group col-xs-12 col-sm-6">
                        <label htmlFor="username">Username</label>
                        <input type="text" name="username" ref={(r) => this.sourceUsername = r} className="form-control" id="username" onChange={this.onInputChange} value={source.username || ''}></input>
                      </div>
                      <div className="form-group col-xs-12 col-sm-6">
                        <label htmlFor="password">Password</label>
                        <input type="password" name="password" ref={(r) => this.sourcePassword = r} className="form-control" id="password" onChange={this.onInputChange} value={source.password || ''}></input>
                      </div>
                      <div className="form-group col-xs-12">
                        <label htmlFor="telegraf">Telegraf database</label>
                        <input type="text" name="telegraf" ref={(r) => this.sourceTelegraf = r} className="form-control" id="telegraf" onChange={this.onInputChange} value={source.telegraf || 'telegraf'}></input>
                      </div>
                      <div className="form-group col-xs-12">
                        <div className="form-control-static">
                          <input type="checkbox" id="defaultSourceCheckbox" defaultChecked={source.default} ref={(r) => this.sourceDefault = r} />
                          <label htmlFor="defaultSourceCheckbox">Make this the default source</label>
                        </div>
                      </div>
                      <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
                        <button className={classNames('btn btn-block', {'btn-primary': editMode, 'btn-success': !editMode})} type="submit">{editMode ? "Save Changes" : "Add Source"}</button>
                      </div>
                    </form>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

function mapStateToProps(_) {
  return {};
}

export default connect(mapStateToProps, {addSourceAction, updateSourceAction})(withRouter(SourceForm));

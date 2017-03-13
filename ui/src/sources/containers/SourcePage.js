import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {getSource} from 'shared/apis';
import {createSource, updateSource} from 'shared/apis';
import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
} from 'shared/actions/sources';
import {connect} from 'react-redux';
import SourceForm from 'src/sources/components/SourceForm';

const {
  func,
  shape,
  string,
} = PropTypes;

export const SourcePage = React.createClass({
  propTypes: {
    params: shape({
      id: string,
      sourceID: string,
    }),
    router: shape({
      push: func.isRequired,
    }).isRequired,
    location: shape({
      query: shape({
        redirectPath: string,
      }).isRequired,
    }).isRequired,
    addFlashMessage: func.isRequired,
    addSourceAction: func,
    updateSourceAction: func,
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

  handleInputChange(e) {
    const val = e.target.value;
    const name = e.target.name;
    this.setState((prevState) => {
      const newSource = Object.assign({}, prevState.source, {
        [name]: val,
      });
      return Object.assign({}, prevState, {source: newSource});
    });
  },

  handleBlurSourceURL(newSource) {
    if (this.state.editMode) {
      return
    }

    if (!newSource.url) {
      return
    }

    createSource(newSource).then(({data: sourceFromServer}) => {
      this.props.addSourceAction(sourceFromServer)
    })
  },

  handleSubmit(newSource) {
    const {router, params, addFlashMessage} = this.props;
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


  render() {
    const {source, editMode} = this.state;
    const {addFlashMessage, router, location, params} = this.props;

    return (
      <SourceForm
        sourceID={params.sourceID}
        router={router}
        location={location}
        source={source}
        editMode={editMode}
        addFlashMessage={addFlashMessage}
        addSourceAction={addSourceAction}
        updateSourceAction={updateSourceAction}
        onInputChange={this.handleInputChange}
        onSubmit={this.handleSubmit}
        onBlurSourceURL={this.handleBlurSourceURL}
      />
    );
  },
});

function mapStateToProps(_) {
  return {};
}

export default connect(mapStateToProps, {addSourceAction, updateSourceAction})(withRouter(SourcePage));

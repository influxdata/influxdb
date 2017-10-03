import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {getSource} from 'shared/apis'
import {createSource, updateSource} from 'shared/apis'
import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
} from 'shared/actions/sources'
import {connect} from 'react-redux'

import SourceForm from 'src/sources/components/SourceForm'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import SourceIndicator from 'shared/components/SourceIndicator'

const {func, shape, string} = PropTypes

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
      error: '',
    }
  },

  componentDidMount() {
    if (!this.state.editMode) {
      return
    }
    getSource(this.props.params.id).then(({data: source}) => {
      this.setState({source})
    })
  },

  handleInputChange(e) {
    const val = e.target.value
    const name = e.target.name
    this.setState(prevState => {
      const newSource = Object.assign({}, prevState.source, {
        [name]: val,
      })
      return Object.assign({}, prevState, {source: newSource})
    })
  },

  handleBlurSourceURL(newSource) {
    if (this.state.editMode) {
      return
    }

    if (!newSource.url) {
      return
    }

    // if there is a type on source it has already been created
    if (newSource.type) {
      return
    }

    createSource(newSource)
      .then(({data: sourceFromServer}) => {
        this.props.addSourceAction(sourceFromServer)
        this.setState({source: sourceFromServer, error: null})
      })
      .catch(({data: error}) => {
        // dont want to flash this until they submit
        this.setState({error: error.message})
      })
  },

  handleSubmit(newSource) {
    const {router, params, addFlashMessage} = this.props
    const {error} = this.state

    if (error) {
      return addFlashMessage({type: 'error', text: error})
    }

    updateSource(newSource)
      .then(({data: sourceFromServer}) => {
        this.props.updateSourceAction(sourceFromServer)
        router.push(`/sources/${params.sourceID}/manage-sources`)
        addFlashMessage({type: 'success', text: 'The source info saved'})
      })
      .catch(() => {
        addFlashMessage({
          type: 'error',
          text: 'There was a problem updating the source. Check the settings',
        })
      })
  },

  render() {
    const {source, editMode} = this.state

    if (editMode && !source.id) {
      return <div className="page-spinner" />
    }

    return (
      <div className="page" id="source-form-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">
                {editMode ? 'Edit Source' : 'Add a New Source'}
              </h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator />
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-8 col-md-offset-2">
                <div className="panel panel-minimal">
                  <SourceForm
                    source={source}
                    editMode={editMode}
                    onInputChange={this.handleInputChange}
                    onSubmit={this.handleSubmit}
                    onBlurSourceURL={this.handleBlurSourceURL}
                  />
                </div>
              </div>
            </div>
          </div>
        </FancyScrollbar>
      </div>
    )
  },
})

function mapStateToProps(_) {
  return {}
}

export default connect(mapStateToProps, {addSourceAction, updateSourceAction})(
  withRouter(SourcePage)
)

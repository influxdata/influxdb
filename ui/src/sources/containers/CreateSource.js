import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'

import {createSource, updateSource} from 'shared/apis'
import SourceForm from 'src/sources/components/SourceForm'
import {addSource as addSourceAction} from 'src/shared/actions/sources'

const {
  func,
  shape,
  string,
} = PropTypes

export const CreateSource = React.createClass({
  propTypes: {
    router: shape({
      push: func.isRequired,
    }).isRequired,
    location: shape({
      query: shape({
        redirectPath: string,
      }).isRequired,
    }).isRequired,
    addSourceAction: func,
    updateSourceAction: func,
  },

  getInitialState() {
    return {
      source: {},
    }
  },

  handleNewSource(e) {
    e.preventDefault()
    const source = {
      url: this.sourceURL.value.trim(),
      name: this.sourceName.value,
      username: this.sourceUser.value,
      password: this.sourcePassword.value,
      isDefault: true,
      telegraf: this.sourceTelegraf.value,
      insecureSkipVerify: this.sourceInsecureSkipVerify ? this.sourceInsecureSkipVerify.checked : false,
      metaUrl: this.metaUrl && this.metaUrl.value.trim(),
    }
    createSource(source).then(({data: sourceFromServer}) => {
      this.props.addSourceAction(sourceFromServer)
      this.redirectToApp(sourceFromServer)
    })
  },

  redirectToApp(source) {
    const {redirectPath} = this.props.location.query
    if (!redirectPath) {
      return this.props.router.push(`/sources/${source.id}/hosts`)
    }

    const fixedPath = redirectPath.replace(/\/sources\/[^/]*/, `/sources/${source.id}`)
    return this.props.router.push(fixedPath)
  },

  handleInputChange(e) {
    const val = e.target.value
    const name = e.target.name
    this.setState((prevState) => {
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

    createSource(newSource).then(({data: sourceFromServer}) => {
      this.props.addSourceAction(sourceFromServer)
      this.setState({source: sourceFromServer})
    }).catch(({data: error}) => {
      // dont want to flash this until they submit
      this.setState({error: error.message})
    })
  },

  handleSubmit(newSource) {
    const {error} = this.state

    if (error) {
      // useful error message
      // return addFlashMessage({type: 'error', text: error})
    }

    updateSource(newSource).then(({data: sourceFromServer}) => {
      this.props.updateSourceAction(sourceFromServer)
      this.redirectToApp(newSource)
    }).catch(() => {
      // give a useful error message to the user
    })
  },

  render() {
    const {source} = this.state

    return (
      <div className="select-source-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-minimal">
                <div className="panel-heading text-center">
                  <h2 className="deluxe">Welcome to Chronograf</h2>
                </div>
                <SourceForm
                  source={source}
                  editMode={false}
                  onInputChange={this.handleInputChange}
                  onSubmit={this.handleSubmit}
                  onBlurSourceURL={this.handleBlurSourceURL}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  },
})

function mapStateToProps(_) {
  return {}
}

export default connect(mapStateToProps, {addSourceAction})(withRouter(CreateSource))

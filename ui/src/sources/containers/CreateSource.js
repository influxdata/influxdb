import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {
  createSource as createSourceAJAX,
  updateSource as updateSourceAJAX,
} from 'shared/apis'
import SourceForm from 'src/sources/components/SourceForm'
import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
} from 'src/shared/actions/sources'
import {publishNotification} from 'src/shared/actions/notifications'

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
    addSource: func,
    updateSource: func,
    notify: func,
  },

  getInitialState() {
    return {
      source: {},
    }
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

    createSourceAJAX(newSource).then(({data: sourceFromServer}) => {
      this.props.addSource(sourceFromServer)
      this.setState({source: sourceFromServer})
    }).catch(({data: error}) => {
      this.setState({error: error.message})
    })
  },

  handleSubmit(newSource) {
    const {error} = this.state
    const {notify, updateSource} = this.props

    if (error) {
      return notify('error', error)
    }

    updateSourceAJAX(newSource).then(({data: sourceFromServer}) => {
      updateSource(sourceFromServer)
      this.redirectToApp(sourceFromServer)
    }).catch(() => {
      notify('error', 'There was a problem updating the source. Check the settings')
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

function mapDispatchToProps(dispatch) {
  return {
    addSource: bindActionCreators(addSourceAction, dispatch),
    updateSource: bindActionCreators(updateSourceAction, dispatch),
    notify: bindActionCreators(publishNotification, dispatch),
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(withRouter(CreateSource))

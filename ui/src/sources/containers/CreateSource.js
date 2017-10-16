import React, {PropTypes, Component} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {
  createSource as createSourceAJAX,
  updateSource as updateSourceAJAX,
} from 'shared/apis'
import SourceForm from 'src/sources/components/SourceForm'
import Notifications from 'shared/components/Notifications'
import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
} from 'shared/actions/sources'
import {publishNotification} from 'shared/actions/notifications'

class CreateSource extends Component {
  constructor(props) {
    super(props)
    this.state = {
      source: {
        url: '',
        name: '',
        username: '',
        password: '',
        default: '',
        telegraf: '',
        insecureSkipVerify: false,
        metaUrl: '',
      },
      error: null,
    }
  }

  redirectToApp = source => {
    const {redirectPath} = this.props.location.query
    if (!redirectPath) {
      return this.props.router.push(`/sources/${source.id}/hosts`)
    }

    const fixedPath = redirectPath.replace(
      /\/sources\/[^/]*/,
      `/sources/${source.id}`
    )
    return this.props.router.push(fixedPath)
  }

  handleInputChange = e => {
    const val = e.target.value
    const name = e.target.name
    this.setState(prevState => {
      const newSource = Object.assign({}, prevState.source, {
        [name]: val,
      })
      return Object.assign({}, prevState, {source: newSource, error: null})
    })
  }

  handleBlurSourceURL = newSource => {
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

    createSourceAJAX(newSource)
      .then(({data: sourceFromServer}) => {
        this.props.addSource(sourceFromServer)
        this.setState({source: sourceFromServer, isCreated: true})
      })
      .catch(({data: error}) => {
        this.setState({error: error.message})
      })
  }

  handleSubmit = newSource => {
    const {notify, updateSource} = this.props
    const {isCreated} = this.state

    if (!isCreated) {
      return createSourceAJAX(newSource)
        .then(({data: sourceFromServer}) => {
          this.props.addSource(sourceFromServer)
          this.setState({source: sourceFromServer, error: null})
          this.redirectToApp(sourceFromServer)
        })
        .catch(({data: error}) => {
          this.setState({error: error.message})
          notify(
            'error',
            `There was a problem creating source: ${error.message}`
          )
        })
    }

    updateSourceAJAX(newSource)
      .then(({data: sourceFromServer}) => {
        updateSource(sourceFromServer)
        this.redirectToApp(sourceFromServer)
      })
      .catch(({data: error}) => {
        notify('error', `There was a problem: ${error.message}`)
      })
  }

  render() {
    const {source} = this.state

    return (
      <div>
        <Notifications />
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
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

CreateSource.propTypes = {
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
}

const mapDispatchToProps = dispatch => ({
  addSource: bindActionCreators(addSourceAction, dispatch),
  updateSource: bindActionCreators(updateSourceAction, dispatch),
  notify: bindActionCreators(publishNotification, dispatch),
})

export default connect(null, mapDispatchToProps)(withRouter(CreateSource))

import React, {PropTypes, Component} from 'react'
import {withRouter} from 'react-router'
import _ from 'lodash'
import {getSource} from 'shared/apis'
import {createSource, updateSource} from 'shared/apis'
import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
} from 'shared/actions/sources'
import {publishNotification} from 'shared/actions/notifications'
import {connect} from 'react-redux'

import SourceForm from 'src/sources/components/SourceForm'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import SourceIndicator from 'shared/components/SourceIndicator'
import {DEFAULT_SOURCE} from 'shared/constants'
const initialPath = '/sources/new'

class SourcePage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isLoading: true,
      source: DEFAULT_SOURCE,
      editMode: props.params.id !== undefined,
      isInitialSource: props.router.location.pathname === initialPath,
    }
  }

  componentDidMount() {
    const {editMode} = this.state
    const {params} = this.props

    if (!editMode) {
      return this.setState({isLoading: false})
    }

    getSource(params.id)
      .then(({data: source}) => {
        this.setState({
          source: {...DEFAULT_SOURCE, ...source},
          isLoading: false,
        })
      })
      .catch(error => {
        this.handleError(error)
        this.setState({isLoading: false})
      })
  }

  handleInputChange = e => {
    let val = e.target.value
    const name = e.target.name

    if (e.target.type === 'checkbox') {
      val = e.target.checked
    }

    this.setState(prevState => {
      const source = {
        ...prevState.source,
        [name]: val,
      }

      return {...prevState, source}
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

    createSource(newSource)
      .then(({data: sourceFromServer}) => {
        this.props.addSourceAction(sourceFromServer)
        this.setState({
          source: {...DEFAULT_SOURCE, ...sourceFromServer},
          isCreated: true,
        })
      })
      .catch(err => {
        // dont want to flash this until they submit
        const error = this.parseError(err)
        console.error('Error on source creation: ', error)
      })
  }

  handleSubmit = e => {
    e.preventDefault()
    const {notify} = this.props
    const {isCreated, source, editMode} = this.state
    const isNewSource = !editMode

    if (!isCreated && isNewSource) {
      return createSource(source)
        .then(({data: sourceFromServer}) => {
          this.props.addSourceAction(sourceFromServer)
          this._redirect(sourceFromServer)
        })
        .catch(error => {
          this.handleError(error)
        })
    }

    updateSource(source)
      .then(({data: sourceFromServer}) => {
        this.props.updateSourceAction(sourceFromServer)
        this._redirect(sourceFromServer)
        notify('success', 'The source info saved')
      })
      .catch(error => {
        this.handleError(error)
      })
  }

  handleError = err => {
    const {notify} = this.props
    const error = this.parseError(err)
    console.error('Error: ', error)
    notify('error', `There was a problem: ${error}`)
  }

  _redirect = source => {
    const {isInitialSource} = this.state
    const {params, router} = this.props

    if (isInitialSource) {
      return this._redirectToApp(source)
    }

    router.push(`/sources/${params.sourceID}/manage-sources`)
  }

  _redirectToApp = source => {
    const {location, router} = this.props
    const {redirectPath} = location.query

    if (!redirectPath) {
      return router.push(`/sources/${source.id}/hosts`)
    }

    const fixedPath = redirectPath.replace(
      /\/sources\/[^/]*/,
      `/sources/${source.id}`
    )
    return router.push(fixedPath)
  }

  parseError = error => {
    return _.get(error, ['data', 'message'], error)
  }

  render() {
    const {isLoading, source, editMode, isInitialSource} = this.state

    if (isLoading) {
      return <div className="page-spinner" />
    }

    return (
      <div className={`${isInitialSource ? '' : 'page'}`}>
        {isInitialSource
          ? null
          : <div className="page-header">
              <div className="page-header__container page-header__source-page">
                <div className="page-header__col-md-8">
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
            </div>}
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
  }
}

const {func, shape, string} = PropTypes

SourcePage.propTypes = {
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
  notify: func,
  addSourceAction: func,
  updateSourceAction: func,
}

const mapStateToProps = () => ({})

export default connect(mapStateToProps, {
  notify: publishNotification,
  addSourceAction,
  updateSourceAction,
})(withRouter(SourcePage))

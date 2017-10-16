import React, {PropTypes, Component} from 'react'
import {withRouter} from 'react-router'
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

class SourcePage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      source: DEFAULT_SOURCE,
      editMode: this.props.params.id !== undefined,
      error: '',
    }
  }

  componentDidMount() {
    if (!this.state.editMode) {
      return
    }

    getSource(this.props.params.id).then(({data: source}) => {
      this.setState({source: {...DEFAULT_SOURCE, ...source}})
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
      .catch(({data: error}) => {
        // dont want to flash this until they submit
        this.setState({error: error.message})
      })
  }

  handleSubmit = e => {
    e.preventDefault()
    const {router, params, notify} = this.props
    const {isCreated, source, editMode} = this.state
    const isNewSource = !editMode

    if (!isCreated && isNewSource) {
      return createSource(source)
        .then(({data: sourceFromServer}) => {
          this.props.addSourceAction(sourceFromServer)
          router.push(`/sources/${params.sourceID}/manage-sources`)
        })
        .catch(({data: error}) => {
          console.error('Error on source creation: ', error.message)
          notify(
            'error',
            `There was a problem creating source: ${error.message}`
          )
        })
    }

    updateSource(source)
      .then(({data: sourceFromServer}) => {
        this.props.updateSourceAction(sourceFromServer)
        router.push(`/sources/${params.sourceID}/manage-sources`)
        notify('success', 'The source info saved')
      })
      .catch(({data: error}) => {
        console.error('Error on source update', error.message)
        notify('error', `There was a problem: ${error.message}`)
      })
  }

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

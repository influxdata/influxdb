import React, {Component, PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {publishNotification as publishNotificationAction} from 'shared/actions/notifications'

import {
  getKapacitor,
  createKapacitor,
  updateKapacitor,
  pingKapacitor,
} from 'shared/apis'
import KapacitorForm from '../components/KapacitorForm'

const defaultName = 'My Kapacitor'
const kapacitorPort = '9092'

class KapacitorPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      kapacitor: {
        url: this._parseKapacitorURL(),
        name: defaultName,
        username: '',
        password: '',
      },
      exists: false,
    }
  }

  componentDidMount() {
    const {source, params: {id}} = this.props
    if (!id) {
      return
    }

    getKapacitor(source, id).then(kapacitor => {
      this.setState({kapacitor})
      this.checkKapacitorConnection(kapacitor)
    })
  }

  checkKapacitorConnection = async kapacitor => {
    try {
      await pingKapacitor(kapacitor)
      this.setState({exists: true})
    } catch (error) {
      this.setState({exists: false})
      this.props.publishNotification({
        type: 'error',
        icon: 'alert-triangle',
        duration: 10000,
        text: 'Could not connect to Kapacitor. Check settings.',
      })
    }
  }

  handleInputChange = e => {
    const {value, name} = e.target

    this.setState(prevState => {
      const update = {[name]: value}
      return {kapacitor: {...prevState.kapacitor, ...update}}
    })
  }

  handleChangeUrl = ({value}) => {
    this.setState({kapacitor: {...this.state.kapacitor, url: value}})
  }

  handleSubmit = e => {
    e.preventDefault()
    const {
      publishNotification,
      source,
      source: {kapacitors = []},
      params,
      router,
    } = this.props
    const {kapacitor} = this.state
    kapacitor.name = kapacitor.name.trim()
    const isNameTaken = kapacitors.some(k => k.name === kapacitor.name)
    const isNew = !params.id

    if (isNew && isNameTaken) {
      publishNotification({
        type: 'error',
        icon: 'alert-triangle',
        duration: 10000,
        text: `There is already a Kapacitor configuration named "${kapacitor.name}"`,
      })
      return
    }

    if (params.id) {
      updateKapacitor(kapacitor)
        .then(({data}) => {
          this.setState({kapacitor: data})
          this.checkKapacitorConnection(data)
          publishNotification({
            type: 'success',
            icon: 'checkmark',
            duration: 5000,
            message: 'Kapacitor Updated!',
          })
        })
        .catch(() => {
          publishNotification({
            type: 'error',
            icon: 'alert-triangle',
            duration: 10000,
            text: 'There was a problem updating the Kapacitor record',
          })
        })
    } else {
      createKapacitor(source, kapacitor)
        .then(({data}) => {
          // need up update kapacitor with info from server to AlertOutputs
          this.setState({kapacitor: data})
          this.checkKapacitorConnection(data)
          router.push(`/sources/${source.id}/kapacitors/${data.id}/edit`)
          publishNotification({
            type: 'success',
            icon: 'checkmark',
            duration: 5000,
            text: 'Kapacitor Created! Configuring endpoints is optional.',
          })
        })
        .catch(() => {
          publishNotification({
            type: 'error',
            icon: 'alert-triangle',
            duration: 10000,
            text: 'There was a problem creating the Kapacitor record',
          })
        })
    }
  }

  handleResetToDefaults = e => {
    e.preventDefault()
    const defaultState = {
      url: this._parseKapacitorURL(),
      name: defaultName,
      username: '',
      password: '',
    }

    this.setState({kapacitor: {...defaultState}})
  }

  _parseKapacitorURL = () => {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${kapacitorPort}`
  }

  render() {
    const {source, location, params} = this.props
    const hash = (location && location.hash) || (params && params.hash) || ''
    const {kapacitor, exists} = this.state
    return (
      <KapacitorForm
        onSubmit={this.handleSubmit}
        onInputChange={this.handleInputChange}
        onChangeUrl={this.handleChangeUrl}
        onReset={this.handleResetToDefaults}
        kapacitor={kapacitor}
        source={source}
        exists={exists}
        hash={hash}
      />
    )
  }
}

const {array, func, shape, string} = PropTypes

KapacitorPage.propTypes = {
  publishNotification: func.isRequired,
  params: shape({
    id: string,
  }).isRequired,
  router: shape({
    push: func.isRequired,
  }).isRequired,
  source: shape({
    id: string.isRequired,
    url: string.isRequired,
    kapacitors: array,
  }),
  location: shape({pathname: string, hash: string}).isRequired,
}

const mapDispatchToProps = dispatch => ({
  publishNotification: bindActionCreators(publishNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(withRouter(KapacitorPage))

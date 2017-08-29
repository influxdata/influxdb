import React, {Component, PropTypes} from 'react'
import {withRouter} from 'react-router'

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

    this.handleInputChange = ::this.handleInputChange
    this.handleSubmit = ::this.handleSubmit
    this.handleResetToDefaults = ::this.handleResetToDefaults
    this._parseKapacitorURL = ::this._parseKapacitorURL
    this.checkKapacitorConnection = ::this.checkKapacitorConnection
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

  async checkKapacitorConnection(kapacitor) {
    try {
      await pingKapacitor(kapacitor)
      this.setState({exists: true})
    } catch (error) {
      this.setState({exists: false})
      this.props.addFlashMessage({
        type: 'error',
        text: 'Could not connect to Kapacitor. Check settings.',
      })
    }
  }

  handleInputChange(e) {
    const {value, name} = e.target

    this.setState(prevState => {
      const update = {[name]: value.trim()}
      return {kapacitor: {...prevState.kapacitor, ...update}}
    })
  }

  handleSubmit(e) {
    e.preventDefault()
    const {
      addFlashMessage,
      source,
      source: {kapacitors = []},
      params,
      router,
    } = this.props
    const {kapacitor} = this.state

    const isNameTaken = kapacitors.some(k => k.name === kapacitor.name)

    if (isNameTaken) {
      addFlashMessage({
        type: 'error',
        text: `There is already a Kapacitor configuration named "${kapacitor.name}"`,
      })
      return
    }

    if (params.id) {
      updateKapacitor(kapacitor)
        .then(({data}) => {
          this.setState({kapacitor: data})
          this.checkKapacitorConnection(data)
          addFlashMessage({type: 'success', text: 'Kapacitor Updated!'})
        })
        .catch(() => {
          addFlashMessage({
            type: 'error',
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
          addFlashMessage({
            type: 'success',
            text: 'Kapacitor Created! Configuring endpoints is optional.',
          })
        })
        .catch(() => {
          addFlashMessage({
            type: 'error',
            text: 'There was a problem creating the Kapacitor record',
          })
        })
    }
  }

  handleResetToDefaults(e) {
    e.preventDefault()
    const defaultState = {
      url: this._parseKapacitorURL(),
      name: defaultName,
      username: '',
      password: '',
    }

    this.setState({kapacitor: {...defaultState}})
  }

  _parseKapacitorURL() {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${kapacitorPort}`
  }

  render() {
    const {source, addFlashMessage} = this.props
    const {kapacitor, exists} = this.state

    return (
      <KapacitorForm
        onSubmit={this.handleSubmit}
        onInputChange={this.handleInputChange}
        onReset={this.handleResetToDefaults}
        kapacitor={kapacitor}
        source={source}
        addFlashMessage={addFlashMessage}
        exists={exists}
      />
    )
  }
}

const {array, func, shape, string} = PropTypes

KapacitorPage.propTypes = {
  addFlashMessage: func,
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
}

export default withRouter(KapacitorPage)

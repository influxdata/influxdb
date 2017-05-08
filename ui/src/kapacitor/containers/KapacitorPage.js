import React, {Component, PropTypes} from 'react'
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
      this.setState({kapacitor}, () => this.checkKapacitorConnection(kapacitor))
    })
  }

  checkKapacitorConnection(kapacitor) {
    pingKapacitor(kapacitor)
      .then(() => {
        this.setState({exists: true})
      })
      .catch(() => {
        this.setState({exists: false})
        this.props.addFlashMessage({
          type: 'error',
          text: 'Could not connect to Kapacitor. Check settings.',
        })
      })
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
    const {addFlashMessage, source} = this.props
    const {kapacitor, exists} = this.state

    if (exists) {
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
          addFlashMessage({type: 'success', text: 'Kapacitor Created!'})
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

const {func, shape, string} = PropTypes

KapacitorPage.propTypes = {
  addFlashMessage: func,
  params: shape({
    id: string,
  }).isRequired,
  source: shape({
    id: string.isRequired,
    url: string.isRequired,
  }),
}

export default KapacitorPage

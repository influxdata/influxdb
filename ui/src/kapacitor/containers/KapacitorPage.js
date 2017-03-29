import React, {PropTypes} from 'react'
import {
  getKapacitor,
  createKapacitor,
  updateKapacitor,
  pingKapacitor,
} from 'shared/apis'
import KapacitorForm from '../components/KapacitorForm'

const defaultName = "My Kapacitor"
const kapacitorPort = "9092"

const {
  func,
  shape,
  string,
} = PropTypes

export const KapacitorPage = React.createClass({
  propTypes: {
    source: shape({
      id: string.isRequired,
      url: string.isRequired,
    }),
    addFlashMessage: func,
  },

  getInitialState() {
    return {
      kapacitor: {
        url: this._parseKapacitorURL(),
        name: defaultName,
        username: '',
        password: '',
      },
      exists: false,
    }
  },

  componentDidMount() {
    const {source} = this.props
    getKapacitor(source).then((kapacitor) => {
      if (!kapacitor) {
        return
      }

      this.setState({kapacitor, exists: true}, () => {
        pingKapacitor(kapacitor).catch(() => {
          this.props.addFlashMessage({type: 'error', text: 'Could not connect to Kapacitor. Check settings.'})
        })
      })
    })
  },

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
  },

  handleInputChange(e) {
    const val = e.target.value
    const name = e.target.name

    this.setState((prevState) => {
      const update = {[name]: val.trim()}
      return {kapacitor: {...prevState.kapacitor, ...update}}
    })
  },


  handleSubmit(e) {
    e.preventDefault()
    const {addFlashMessage, source} = this.props
    const {kapacitor, exists} = this.state

    if (exists) {
      updateKapacitor(kapacitor).then(() => {
        addFlashMessage({type: 'success', text: 'Kapacitor Updated!'})
      }).catch(() => {
        addFlashMessage({type: 'error', text: 'There was a problem updating the Kapacitor record'})
      })
    } else {
      createKapacitor(source, kapacitor).then(({data}) => {
        // need up update kapacitor with info from server to AlertOutputs
        this.setState({kapacitor: data, exists: true})
        addFlashMessage({type: 'success', text: 'Kapacitor Created!'})
      }).catch(() => {
        addFlashMessage({type: 'error', text: 'There was a problem creating the Kapacitor record'})
      })
    }
  },

  handleResetToDefaults(e) {
    e.preventDefault()
    const defaultState = {
      url: this._parseKapacitorURL(),
      name: defaultName,
      username: '',
      password: '',
    }

    this.setState({kapacitor: {...defaultState}})
  },

  _parseKapacitorURL() {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${kapacitorPort}`
  },
})

export default KapacitorPage

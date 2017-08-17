import React, {PropTypes, Component} from 'react'

import RedactedInput from './RedactedInput'

class VictorOpsConfig extends Component {
  constructor(props) {
    super(props)
  }

  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      'api-key': this.apiKey.value,
      'routing-key': this.routingKey.value,
      url: this.url.value,
    }

    this.props.onSave(properties)
  }

  handleApiRef = r => (this.apiKey = r)

  render() {
    const {options} = this.props.config
    const apiKey = options['api-key']
    const routingKey = options['routing-key']
    const {url} = options

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="api-key">API Key</label>
          <RedactedInput
            defaultValue={apiKey}
            id="api-key"
            refFunc={this.handleApiRef}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="routing-key">Routing Key</label>
          <input
            className="form-control"
            id="routing-key"
            type="text"
            ref={r => (this.routingKey = r)}
            defaultValue={routingKey || ''}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="url">VictorOps URL</label>
          <input
            className="form-control"
            id="url"
            type="text"
            ref={r => (this.url = r)}
            defaultValue={url || ''}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update VictorOps Config
          </button>
        </div>
      </form>
    )
  }
}

const {bool, shape, string, func} = PropTypes

VictorOpsConfig.propTypes = {
  config: shape({
    options: shape({
      'api-key': bool,
      'routing-key': string,
      url: string,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
}

export default VictorOpsConfig

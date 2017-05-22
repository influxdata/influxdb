import React, {PropTypes} from 'react'

import RedactedInput from './RedactedInput'

const VictorOpsConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        'api-key': PropTypes.bool,
        'routing-key': PropTypes.string,
        url: PropTypes.string,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      'api-key': this.apiKey.value,
      'routing-key': this.routingKey.value,
      url: this.url.value,
    }

    this.props.onSave(properties)
  },

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
            refFunc={r => (this.apiKey = r)}
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
  },
})

export default VictorOpsConfig

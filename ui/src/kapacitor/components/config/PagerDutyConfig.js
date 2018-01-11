import React, {PropTypes, Component} from 'react'
import RedactedInput from './RedactedInput'

class PagerDutyConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  handleSubmit = e => {
    e.preventDefault()

    const properties = {
      'service-key': this.serviceKey.value,
      url: this.url.value,
    }

    this.props.onSave(properties)
    this.setState({testEnabled: true})
  }

  disableTest = () => {
    this.setState({testEnabled: false})
  }

  render() {
    const {options} = this.props.config
    const {url} = options
    const serviceKey = options['service-key']
    const refFunc = r => (this.serviceKey = r)
    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="service-key">Service Key</label>
          <RedactedInput
            defaultValue={serviceKey || ''}
            id="service-key"
            refFunc={refFunc}
            disableTest={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="url">PagerDuty URL</label>
          <input
            className="form-control"
            id="url"
            type="text"
            ref={r => (this.url = r)}
            defaultValue={url || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={this.state.testEnabled}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={!this.state.testEnabled}
            onClick={this.props.onTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
      </form>
    )
  }
}

const {bool, func, shape, string} = PropTypes

PagerDutyConfig.propTypes = {
  config: shape({
    options: shape({
      'service-key': bool.isRequired,
      url: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
  onTest: func.isRequired,
  enabled: bool.isRequired,
}

export default PagerDutyConfig

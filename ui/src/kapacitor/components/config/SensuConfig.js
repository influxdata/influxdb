import React, {PropTypes, Component} from 'react'

class SensuConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      source: this.source.value,
      addr: this.addr.value,
    }

    const success = await this.props.onSave(properties)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  disableTest = () => {
    this.setState({testEnabled: false})
  }

  render() {
    const {source, addr} = this.props.config.options

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="source">Source</label>
          <input
            className="form-control"
            id="source"
            type="text"
            ref={r => (this.source = r)}
            defaultValue={source || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="address">Address</label>
          <input
            className="form-control"
            id="address"
            type="text"
            ref={r => (this.addr = r)}
            defaultValue={addr || ''}
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

SensuConfig.propTypes = {
  config: shape({
    options: shape({
      source: string.isRequired,
      addr: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
  onTest: func.isRequired,
  enabled: bool.isRequired,
}

export default SensuConfig

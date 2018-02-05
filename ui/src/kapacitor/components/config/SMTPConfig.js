import React, {PropTypes, Component} from 'react'

class SMTPConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      host: this.host.value,
      port: this.port.value,
      from: this.from.value,
      username: this.username.value,
      password: this.password.value,
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
    const {host, port, from, username, password} = this.props.config.options

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="smtp-host">SMTP Host</label>
          <input
            className="form-control"
            id="smtp-host"
            type="text"
            ref={r => (this.host = r)}
            defaultValue={host || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="smtp-port">SMTP Port</label>
          <input
            className="form-control"
            id="smtp-port"
            type="text"
            ref={r => (this.port = r)}
            defaultValue={port || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="smtp-from">From Email</label>
          <input
            className="form-control"
            id="smtp-from"
            placeholder="email@domain.com"
            type="text"
            ref={r => (this.from = r)}
            defaultValue={from || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="smtp-user">User</label>
          <input
            className="form-control"
            id="smtp-user"
            type="text"
            ref={r => (this.username = r)}
            defaultValue={username || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="smtp-password">Password</label>
          <input
            className="form-control"
            id="smtp-password"
            type="password"
            ref={r => (this.password = r)}
            defaultValue={`${password}`}
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

const {bool, func, number, shape, string} = PropTypes

SMTPConfig.propTypes = {
  config: shape({
    options: shape({
      host: string,
      port: number,
      username: string,
      password: bool,
      from: string,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
  onTest: func.isRequired,
  enabled: bool.isRequired,
}

export default SMTPConfig

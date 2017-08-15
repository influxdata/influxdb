import React, {PropTypes, Component} from 'react'

class SMTPConfig extends Component {
  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      host: this.host.value,
      port: this.port.value,
      from: this.from.value,
      username: this.username.value,
      password: this.password.value,
    }

    this.props.onSave(properties)
  }

  render() {
    const {host, port, from, username, password} = this.props.config.options

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="smtp-host">SMTP Host</label>
          <input
            className="form-control"
            id="smtp-host"
            type="text"
            ref={r => (this.host = r)}
            defaultValue={host || ''}
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
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update SMTP Config
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
}

export default SMTPConfig

import React, {PropTypes} from 'react'

const SMTPConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        host: PropTypes.string,
        port: PropTypes.number,
        username: PropTypes.string,
        password: PropTypes.bool,
        from: PropTypes.string,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      host: this.host.value,
      port: this.port.value,
      from: this.from.value,
      username: this.username.value,
      password: this.password.value,
    }

    this.props.onSave(properties)
  },

  render() {
    const {host, port, from, username, password} = this.props.config.options

    return (
      <div>
        <h4 className="text-center no-user-select">SMTP Alert</h4>
        <br/>
        <p className="no-user-select">You can have alerts sent to an email address by setting up an SMTP endpoint.</p>
        <form onSubmit={this.handleSaveAlert}>
          <div className="form-group col-xs-12 col-md-6">
            <label htmlFor="smtp-host">SMTP Host</label>
            <input className="form-control" id="smtp-host" type="text" ref={(r) => this.host = r} defaultValue={host || ''}></input>
          </div>

          <div className="form-group col-xs-12 col-md-6">
            <label htmlFor="smtp-port">SMTP Port</label>
            <input className="form-control" id="smtp-port" type="text" ref={(r) => this.port = r} defaultValue={port || ''}></input>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="smtp-from">From Email</label>
            <input className="form-control" id="smtp-from" placeholder="email@domain.com" type="text" ref={(r) => this.from = r} defaultValue={from || ''}></input>
          </div>

          <div className="form-group col-xs-12 col-md-6">
            <label htmlFor="smtp-user">User</label>
            <input className="form-control" id="smtp-user" type="text" ref={(r) => this.username = r} defaultValue={username || ''}></input>
          </div>

          <div className="form-group col-xs-12 col-md-6">
            <label htmlFor="smtp-password">Password</label>
            <input className="form-control" id="smtp-password" type="password" ref={(r) => this.password = r} defaultValue={`${password}`}></input>
          </div>

          <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
            <button className="btn btn-block btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    )
  },
})

export default SMTPConfig

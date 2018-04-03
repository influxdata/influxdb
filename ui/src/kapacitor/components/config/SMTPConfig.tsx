import React, {PureComponent} from 'react'
import {Input} from 'src/types/kapacitor'

interface Properties {
  host: string
  port: string
  from: string
  to: string[]
  username: string
  password: string
}

interface Config {
  options: {
    host: string
    port: number
    username: string
    password: boolean
    from: string
    to: string
  }
}

interface Props {
  config: Config
  onSave: (properties: Properties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  testEnabled: boolean
}

class SMTPConfig extends PureComponent<Props, State> {
  private host: Input
  private port: Input
  private from: Input
  private to: Input
  private username: Input
  private password: Input

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  public render() {
    const {host, port, from, username, password, to} = this.props.config.options

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
            defaultValue={port.toString() || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-6">
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
          <label htmlFor="smtp-to">To Email</label>
          <input
            className="form-control"
            id="smtp-to"
            placeholder="email@domain.com"
            type="text"
            ref={r => (this.to = r)}
            defaultValue={to || ''}
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

        <div className="form-group form-group-submit col-xs-12 text-center">
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

  private handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      host: this.host.value,
      port: this.port.value,
      from: this.from.value,
      to: this.to.value ? [this.to.value] : [],
      username: this.username.value,
      password: this.password.value,
    }
    const success = await this.props.onSave(properties)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }
}

export default SMTPConfig

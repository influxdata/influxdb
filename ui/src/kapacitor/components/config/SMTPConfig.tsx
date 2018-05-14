import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SMTPProperties} from 'src/types/kapacitor'

interface Config {
  options: {
    host: string
    port: number
    username: string
    password: boolean
    from: string
    to: string | string[]
    enabled: boolean
  }
}

interface Props {
  config: Config
  onSave: (properties: SMTPProperties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  testEnabled: boolean
  enabled: boolean
}

@ErrorHandling
class SMTPConfig extends PureComponent<Props, State> {
  private host: HTMLInputElement
  private port: HTMLInputElement
  private from: HTMLInputElement
  private to: HTMLInputElement
  private username: HTMLInputElement
  private password: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
      enabled: _.get(this.props, 'config.options.enabled', false),
    }
  }

  public render() {
    const {host, port, from, username, password, to} = _.get(
      this.props,
      'config.options',
      {}
    )
    const {enabled} = this.state

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

        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              type="checkbox"
              id="disabled"
              checked={enabled}
              onChange={this.handleEnabledChange}
            />
            <label htmlFor="disabled">Configuration Enabled</label>
          </div>
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
            disabled={!this.state.testEnabled || !enabled}
            onClick={this.props.onTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
      </form>
    )
  }

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const properties: SMTPProperties = {
      host: this.host.value,
      port: this.port.value,
      from: this.from.value,
      to: this.to.value ? [this.to.value] : [],
      username: this.username.value,
      password: this.password.value,
      enabled: this.state.enabled,
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

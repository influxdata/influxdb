import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {PUSHOVER_DOCS_LINK} from 'src/kapacitor/copy'
import {PushoverProperties} from 'src/types/kapacitor'

interface Config {
  options: {
    token: boolean
    'user-key': boolean
    url: string
    enabled: boolean
  }
}

interface Props {
  config: Config
  onSave: (properties: PushoverProperties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  testEnabled: boolean
  enabled: boolean
}

@ErrorHandling
class PushoverConfig extends PureComponent<Props, State> {
  private token: HTMLInputElement
  private url: HTMLInputElement
  private userKey: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
      enabled: _.get(this.props, 'config.options.enabled', false),
    }
  }

  public render() {
    const {options} = this.props.config
    const {token, url} = options
    const userKey = options['user-key']
    const {testEnabled, enabled} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="user-key">
            User Key
            <QuestionMarkTooltip
              tipID="token"
              tipContent={PUSHOVER_DOCS_LINK}
            />
          </label>
          <RedactedInput
            defaultValue={userKey}
            id="user-key"
            refFunc={this.handleUserKeyRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="token">
            Token
            <QuestionMarkTooltip
              tipID="token"
              tipContent={PUSHOVER_DOCS_LINK}
            />
          </label>
          <RedactedInput
            defaultValue={token}
            id="token"
            refFunc={this.handleTokenRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="url">Pushover URL</label>
          <input
            className="form-control"
            id="url"
            type="text"
            ref={r => (this.url = r)}
            defaultValue={url || ''}
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

    const properties: PushoverProperties = {
      token: this.token.value,
      url: this.url.value,
      'user-key': this.userKey.value,
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

  private handleUserKeyRef = r => (this.userKey = r)

  private handleTokenRef = r => (this.token = r)
}

export default PushoverConfig

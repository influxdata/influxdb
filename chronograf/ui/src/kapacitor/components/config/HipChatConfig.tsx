import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import {HIPCHAT_TOKEN_TIP} from 'src/kapacitor/copy'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {HipChatProperties} from 'src/types/kapacitor'

interface Config {
  options: {
    room: string
    token: boolean
    url: string
    enabled: boolean
  }
}

interface Props {
  config: Config
  onSave: (properties: HipChatProperties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  testEnabled: boolean
  enabled: boolean
}

@ErrorHandling
class HipchatConfig extends PureComponent<Props, State> {
  private room: HTMLInputElement
  private token: HTMLInputElement
  private url: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
      enabled: _.get(this.props, 'config.options.enabled', false),
    }
  }

  public render() {
    const {options} = this.props.config
    const {url, room, token} = options
    const {testEnabled, enabled} = this.state

    const subdomain = url
      .replace('https://', '')
      .replace('.hipchat.com/v2/room', '')

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">Subdomain</label>
          <input
            className="form-control"
            id="url"
            type="text"
            placeholder="your-subdomain"
            ref={r => (this.url = r)}
            defaultValue={subdomain && subdomain.length ? subdomain : ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="room">Room</label>
          <input
            className="form-control"
            id="room"
            type="text"
            placeholder="your-hipchat-room"
            ref={r => (this.room = r)}
            defaultValue={room || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="token">
            Token
            <QuestionMarkTooltip tipID="token" tipContent={HIPCHAT_TOKEN_TIP} />
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

    const properties: HipChatProperties = {
      room: this.room.value,
      url: `https://${this.url.value}.hipchat.com/v2/room`,
      token: this.token.value,
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

  private handleTokenRef = r => (this.token = r)
}

export default HipchatConfig

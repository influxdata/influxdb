import React, {PureComponent} from 'react'

import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {PUSHOVER_DOCS_LINK} from 'src/kapacitor/copy'

interface Properties {
  token: string
  url: string
  'user-key': string
}

interface Config {
  options: {
    token: boolean
    'user-key': boolean
    url: string
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

@ErrorHandling
class PushoverConfig extends PureComponent<Props, State> {
  private token: HTMLInputElement
  private url: HTMLInputElement
  private userKey: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  public render() {
    const {options} = this.props.config
    const {token, url} = options
    const userKey = options['user-key']
    const {testEnabled} = this.state

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
      token: this.token.value,
      url: this.url.value,
      'user-key': this.userKey.value,
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

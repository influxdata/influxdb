import React, {Component, PropTypes} from 'react'

import QuestionMarkTooltip from 'shared/components/QuestionMarkTooltip'
import RedactedInput from './RedactedInput'

import {PUSHOVER_DOCS_LINK} from 'src/kapacitor/copy'

class PushoverConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  handleSubmit = e => {
    e.preventDefault()

    const properties = {
      token: this.token.value,
      url: this.url.value,
      'user-key': this.userKey.value,
    }

    this.props.onSave(properties)
    this.setState({testEnabled: true})
  }

  disableTest = () => {
    this.setState({testEnabled: false})
  }

  handleUserKeyRef = r => (this.userKey = r)

  handleTokenRef = r => (this.token = r)

  render() {
    const {options} = this.props.config
    const {token, url} = options
    const userKey = options['user-key']

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

PushoverConfig.propTypes = {
  config: shape({
    options: shape({
      token: bool.isRequired,
      'user-key': bool.isRequired,
      url: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
  onTest: func.isRequired,
  enabled: bool.isRequired,
}

export default PushoverConfig

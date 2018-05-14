import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Properties {
  'routing-key': string
  url: string
}

interface Config {
  options: {
    'routing-key': boolean
    url: string
    enabled: boolean
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
  enabled: boolean
}

@ErrorHandling
class PagerDutyConfig extends PureComponent<Props, State> {
  private routingKey: HTMLInputElement
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
    const {url} = options
    const routingKey = options['routing-key']
    const {testEnabled, enabled} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="routing-key">Routing Key</label>
          <RedactedInput
            defaultValue={routingKey}
            id="routing-key"
            refFunc={this.handleRoutingKeyRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="url">PagerDuty URL</label>
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

  private handleRoutingKeyRef = r => (this.routingKey = r)

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      'routing-key': this.routingKey.value,
      url: this.url.value,
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

export default PagerDutyConfig

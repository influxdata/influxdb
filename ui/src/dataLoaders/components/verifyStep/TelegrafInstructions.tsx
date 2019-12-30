// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import CodeSnippet from 'src/shared/components/CodeSnippet'

export interface Props {
  token: string
  configID: string
}

@ErrorHandling
class TelegrafInstructions extends PureComponent<Props> {
  public render() {
    const {token, configID} = this.props
    const exportToken = `export INFLUX_TOKEN=${token || ''}`
    const configScript = `telegraf --config ${
      this.origin
    }/api/v2/telegrafs/${configID || ''}`
    return (
      <div data-testid="setup-instructions" className="telegraf-instructions">
        <h6>1. Install the Latest Telegraf</h6>
        <p>
          You can install the latest Telegraf by visiting the{' '}
          <a
            href="https://portal.influxdata.com/downloads/"
            target="_blank"
            rel="noopener noreferrer"
          >
            InfluxData Downloads&nbsp;
          </a>
          page. If you already have Telegraf installed on your system, make sure
          it's up to date. You will need version 1.9.2 or higher.
        </p>
        <h6>2. Configure your API Token</h6>
        <p>
          Your API token is required for pushing data into InfluxDB. You can
          copy the following command to your terminal window to set an
          environment variable with your token.
        </p>
        <CodeSnippet copyText={exportToken} label="CLI" />
        <h6>3. Start Telegraf</h6>
        <p>
          Finally, you can run the following command to start the Telegraf agent
          running on your machine.
        </p>
        <CodeSnippet copyText={configScript} label="CLI" />
      </div>
    )
  }

  private get origin(): string {
    return window.location.origin
  }
}

export default TelegrafInstructions

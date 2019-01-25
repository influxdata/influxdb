// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import CopyText from 'src/shared/components/CopyText'

// Types
import {NotificationAction} from 'src/types'

export interface Props {
  notify: NotificationAction
  authToken: string
  configID: string
}

@ErrorHandling
class TelegrafInstructions extends PureComponent<Props> {
  public render() {
    const {notify, authToken, configID} = this.props
    const exportToken = `export INFLUX_TOKEN=${authToken || ''}`
    const configScript = `telegraf -config http://localhost:9999/api/v2/telegrafs/${configID ||
      ''}`
    return (
      <>
        <h3 className="wizard-step--title">Listen for Telegraf Data</h3>
        <h5 className="wizard-step--sub-title">
          Get started pushing data into InfluxDB using our open source Telegraf
          agent.
        </h5>
        <div className="wizard-step--body">
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
            page . If you already have Telegraf installed on your system, make
            sure it's up to date. You will need version 1.9.2 or higher.
          </p>
          <h6>2. Configure your API Token</h6>
          <p>
            Your API token is required for pushing data into InfluxDB. You can
            copy the following command to your terminal window to set an
            environment variable with your token.
          </p>
          <CopyText copyText={exportToken} notify={notify} />
          <h6>3. Start Telegraf</h6>
          <p>
            Finally, you can run the following command the start Telegraf agent
            running on your machine.
          </p>
          <CopyText copyText={configScript} notify={notify} />
        </div>
      </>
    )
  }
}

export default TelegrafInstructions

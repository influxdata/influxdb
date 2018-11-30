// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'


export interface Props {
  authToken: string
  configID: string
}

@ErrorHandling
class TelegrafInstructions extends PureComponent<Props> {
  public render() {
    return (
        <>
        <h3 className="wizard-step--title">Listen for Streaming Data</h3>
          <h5 className="wizard-step--sub-title">
            You have selected streaming data sources. Follow the instructions
            below to begin listening for incoming data.
          </h5>
        <div className="wizard-step--body">
          <h6>Install</h6>
          <p>
            You can download the binaries directly from the downloads page or
            from the releases section.{' '}
          </p>
          <h6>Start Data Stream</h6>
          <p>
            After installing the telegraf client, save this environment
            variable. run the following command.
          </p>
          <p className="wizard-step--body-snippet">export INFLUX_TOKEN={this.props.authToken}</p>
          <p>Run the following command.</p>
          <p className="wizard-step--body-snippet">
            telegraf -config http://localhost:9999/api/v2/telegrafs/{this.props.configID}
          </p>
        </div>
        </>)
  }
}

export default TelegrafInstructions

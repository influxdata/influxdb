// Libraries
import React, {PureComponent} from 'react'

// Types
import {ConnectorState} from 'src/clockface/constants/wizard'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  status: ConnectorState
}

@ErrorHandling
class ProgressConnector extends PureComponent<Props> {
  public render() {
    const {status} = this.props

    return (
      <span
        className={`wizard-progress-connector wizard-progress-connector--${status ||
          ConnectorState.None}`}
      />
    )
  }
}

export default ProgressConnector

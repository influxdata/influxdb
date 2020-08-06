// Libraries
import React, {PureComponent} from 'react'

// Components
import {Form, Overlay} from '@influxdata/clockface'
import StatusIndicator from 'src/buckets/components/lineProtocol/verify/StatusIndicator'
import LineProtocolHelperText from 'src/buckets/components/lineProtocol/LineProtocolHelperText'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onExit: () => void
}

@ErrorHandling
export class VerifyLineProtocolStep extends PureComponent<Props> {
  public render() {
    const {onExit} = this.props

    return (
      <Form onSubmit={onExit}>
        <Overlay.Body style={{textAlign: 'center'}}>
          <StatusIndicator />
          <LineProtocolHelperText />
        </Overlay.Body>
      </Form>
    )
  }
}

export default VerifyLineProtocolStep

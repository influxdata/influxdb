// Libraries
import React, {FC} from 'react'

// Components
import {Form, Overlay} from '@influxdata/clockface'
import LineProtocolTabs from 'src/buckets/components/lineProtocol/configure/LineProtocolTabs'
import LineProtocolHelperText from 'src/buckets/components/lineProtocol/LineProtocolHelperText'

// Types
import {LineProtocolTab} from 'src/types/index'

type OwnProps = {onSubmit: () => void}
type Props = OwnProps

const tabs: LineProtocolTab[] = ['Upload File', 'Enter Manually']

const LineProtocol: FC<Props> = ({onSubmit}) => {
  return (
    <Form>
      <Overlay.Body style={{textAlign: 'center'}}>
        <LineProtocolTabs tabs={tabs} onSubmit={onSubmit} />
        <LineProtocolHelperText />
      </Overlay.Body>
    </Form>
  )
}

export default LineProtocol

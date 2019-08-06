// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input} from '@influxdata/clockface'

interface Props {
  messageTemplate: string
  onChange: (e: ChangeEvent) => void
}

const PagerDutyMessage: FC<Props> = ({messageTemplate, onChange}) => {
  // TODO: change this to a TextArea once clockface is fixed
  return (
    <Form.Element label="message">
      <Input
        name="messageTemplate"
        onChange={onChange}
        value={messageTemplate}
      />
    </Form.Element>
  )
}

export default PagerDutyMessage

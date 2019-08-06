// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, TextArea} from '@influxdata/clockface'

interface Props {
  messageTemplate: string
  onChange: (e: ChangeEvent) => void
}

const PagerDutyMessage: FC<Props> = ({messageTemplate, onChange}) => {
  return (
    <Form.Element label="Message">
      <TextArea
        name="messageTemplate"
        onChange={onChange}
        value={messageTemplate}
      />
    </Form.Element>
  )
}

export default PagerDutyMessage

// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input, TextArea} from '@influxdata/clockface'

interface Props {
  to: string
  subjectTemplate
  bodyTemplate: string
  onChange: (e: ChangeEvent) => void
}

const SMTPMessage: FC<Props> = ({
  to,
  subjectTemplate,
  bodyTemplate,
  onChange,
}) => {
  return (
    <>
      <Form.Element label="To">
        <Input value={to} name="to" onChange={onChange} />
      </Form.Element>
      <Form.Element label="Subject">
        <Input
          value={subjectTemplate}
          name="subjectTemplate"
          onChange={onChange}
        />
      </Form.Element>
      <Form.Element label="Body">
        <TextArea
          name="bodyTemplate"
          value={bodyTemplate}
          onChange={onChange}
          rows={3}
        />
      </Form.Element>
    </>
  )
}

export default SMTPMessage

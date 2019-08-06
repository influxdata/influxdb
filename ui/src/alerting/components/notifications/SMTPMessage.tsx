// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input} from '@influxdata/clockface'

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
      <Form.Element label="to">
        <Input value={to} name="to" onChange={onChange} />
      </Form.Element>
      <Form.Element label="subject">
        <Input
          value={subjectTemplate}
          name="subjectTemplate"
          onChange={onChange}
        />
      </Form.Element>
      <Form.Element label="body">
        {/*  TODO: change this to a TextArea once clockface is fixed */}
        <Input name="bodyTemplate" value={bodyTemplate} onChange={onChange} />
      </Form.Element>
    </>
  )
}

export default SMTPMessage

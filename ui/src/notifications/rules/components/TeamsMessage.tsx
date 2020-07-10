// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input, TextArea} from '@influxdata/clockface'

interface Props {
  title: string
  messageTemplate: string
  onChange: (e: ChangeEvent) => void
}

const TeamsMessage: FC<Props> = ({title, messageTemplate, onChange}) => {
  return (
    <>
      <Form.Element label="Title">
        <Input value={title} name="title" onChange={onChange} />
      </Form.Element>
      <Form.Element label="Message Template">
        <TextArea
          name="messageTemplate"
          testID="teams-message-template--textarea"
          value={messageTemplate}
          onChange={onChange}
          rows={3}
        />
      </Form.Element>
    </>
  )
}

export default TeamsMessage

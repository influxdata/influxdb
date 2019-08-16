// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input, TextArea} from '@influxdata/clockface'

interface Props {
  channel: string
  messageTemplate: string
  onChange: (e: ChangeEvent) => void
}

const SlackMessage: FC<Props> = ({channel, messageTemplate, onChange}) => {
  return (
    <>
      <Form.Element label="Channel">
        <Input value={channel} name="channel" onChange={onChange} />
      </Form.Element>
      <Form.Element label="Message Template">
        <TextArea
          name="messageTemplate"
          value={messageTemplate}
          onChange={onChange}
          rows={3}
        />
      </Form.Element>
    </>
  )
}

export default SlackMessage

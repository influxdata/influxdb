// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, Input} from '@influxdata/clockface'

interface Props {
  channel: string
  messageTemplate: string
  onChange: (e: ChangeEvent) => void
}

const SlackMessage: FC<Props> = ({channel, messageTemplate, onChange}) => {
  return (
    <>
      <Form.Element label="channel">
        <Input value={channel} name="channel" onChange={onChange} />
      </Form.Element>
      <Form.Element label="message">
        {/*  TODO: change this to a TextArea once clockface is fixed */}
        <Input
          name="messageTemplate"
          value={messageTemplate}
          onChange={onChange}
        />
      </Form.Element>
    </>
  )
}

export default SlackMessage

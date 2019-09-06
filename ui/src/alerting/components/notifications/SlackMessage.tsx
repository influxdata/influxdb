// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Form, TextArea} from '@influxdata/clockface'

interface Props {
  channel: string
  messageTemplate: string
  onChange: (e: ChangeEvent) => void
}

const SlackMessage: FC<Props> = ({messageTemplate, onChange}) => {
  return (
    <>
      <Form.Element label="Message Template">
        <TextArea
          name="messageTemplate"
          testID="slack-message-template--textarea"
          value={messageTemplate}
          onChange={onChange}
          rows={3}
        />
      </Form.Element>
    </>
  )
}

export default SlackMessage

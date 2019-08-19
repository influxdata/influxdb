// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, FormElement} from '@influxdata/clockface'

interface Props {
  url: string
  routingKey: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsPagerDuty: FC<Props> = ({url, routingKey, onChange}) => {
  return (
    <>
      <FormElement label="URL">
        <Input
          name="url"
          value={url}
          testID="pagerduty-url"
          onChange={onChange}
        />
      </FormElement>
      <FormElement label="Routing Key">
        <Input
          name="routingKey"
          value={routingKey}
          testID="pagerduty-routing-key"
          onChange={onChange}
        />
      </FormElement>
    </>
  )
}

export default EndpointOptionsPagerDuty

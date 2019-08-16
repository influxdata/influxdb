// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, FormElement} from '@influxdata/clockface'

interface Props {
  url: string
  token: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsPagerDuty: FC<Props> = ({url, token, onChange}) => {
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
      <FormElement label="Token">
        <Input
          name="token"
          value={token}
          testID="pagerduty-token"
          onChange={onChange}
        />
      </FormElement>
    </>
  )
}

export default EndpointOptionsPagerDuty

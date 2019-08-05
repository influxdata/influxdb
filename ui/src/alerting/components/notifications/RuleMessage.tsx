// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Form} from '@influxdata/clockface'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'
import RuleEndpointDropdown from 'src/alerting/components/notifications/RuleEndpointDropdown'

// Types
import {NotificationEndpoint, NotificationRuleBox} from 'src/types'

interface Props {
  endpoints: NotificationEndpoint[]
  rule: NotificationRuleBox
}

const RuleMessage: FC<Props> = ({endpoints, rule}) => {
  const dispatch = useContext(NewRuleDispatch)
  const onSelectEndpoint = notifyEndpointID => {
    dispatch({
      type: 'UPDATE_RULE',
      rule: {
        ...rule,
        notifyEndpointID,
      },
    })
  }

  return (
    <>
      <h2 className="cf-overlay--title">Rule Message</h2>
      <Form.Element label="Endpoint">
        <RuleEndpointDropdown
          endpoints={endpoints}
          onSelectEndpoint={onSelectEndpoint}
          selectedEndpointID={rule.notifyEndpointID}
        />
      </Form.Element>
    </>
  )
}

export default RuleMessage

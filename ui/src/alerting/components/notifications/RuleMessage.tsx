// Libraries
import React, {FC} from 'react'

// Components
import {Form, Panel, Grid, Columns} from '@influxdata/clockface'
import RuleEndpointDropdown from 'src/alerting/components/notifications/RuleEndpointDropdown'
import RuleMessageContents from 'src/alerting/components/notifications/RuleMessageContents'

// Types
import {NotificationEndpoint, NotificationRuleDraft} from 'src/types'

// Utils
import {getEndpointBase} from 'src/alerting/components/notifications/utils'

// Hooks
import {useRuleDispatch} from 'src/shared/hooks'

interface Props {
  endpoints: NotificationEndpoint[]
  rule: NotificationRuleDraft
}

const RuleMessage: FC<Props> = ({endpoints, rule}) => {
  const dispatch = useRuleDispatch()
  const onSelectEndpoint = notifyEndpointID => {
    const endpoint = getEndpointBase(endpoints, notifyEndpointID)

    dispatch({
      type: 'UPDATE_RULE',
      rule: {
        ...rule,
        ...endpoint,
        notifyEndpointID,
      },
    })
  }

  return (
    <Grid.Row>
      <Grid.Column widthSM={Columns.Two}>Message</Grid.Column>
      <Grid.Column widthSM={Columns.Ten}>
        <Panel>
          <Panel.Body>
            <Form.Element label="Endpoint">
              <RuleEndpointDropdown
                endpoints={endpoints}
                onSelectEndpoint={onSelectEndpoint}
                selectedEndpointID={rule.notifyEndpointID}
              />
            </Form.Element>
            <RuleMessageContents rule={rule} />
          </Panel.Body>
        </Panel>
      </Grid.Column>
    </Grid.Row>
  )
}

export default RuleMessage

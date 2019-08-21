// Libraries
import React, {FC} from 'react'

// Components
import {Form, Panel, Grid, Columns} from '@influxdata/clockface'
import RuleEndpointDropdown from 'src/alerting/components/notifications/RuleEndpointDropdown'
import RuleMessageContents from 'src/alerting/components/notifications/RuleMessageContents'

// Utils
import {getRuleVariantDefaults} from 'src/alerting/components/notifications/utils'
import {useRuleDispatch} from './RuleOverlay.reducer'

// Types
import {NotificationEndpoint, NotificationRuleDraft} from 'src/types'

interface Props {
  endpoints: NotificationEndpoint[]
  rule: NotificationRuleDraft
}

const RuleMessage: FC<Props> = ({endpoints, rule}) => {
  const dispatch = useRuleDispatch()

  const onSelectEndpoint = endpointID => {
    dispatch({
      type: 'UPDATE_RULE',
      rule: {
        ...rule,
        ...getRuleVariantDefaults(endpoints, endpointID),
        endpointID,
      },
    })
  }

  return (
    <Grid.Row>
      <Grid.Column widthSM={Columns.Two}>Message</Grid.Column>
      <Grid.Column widthSM={Columns.Ten}>
        <Panel>
          <Panel.Body>
            <Form.Element label="Notification Endpoint">
              <RuleEndpointDropdown
                endpoints={endpoints}
                onSelectEndpoint={onSelectEndpoint}
                selectedEndpointID={rule.endpointID}
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

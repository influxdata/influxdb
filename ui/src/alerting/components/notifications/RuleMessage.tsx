// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Form, Panel, Grid, Columns} from '@influxdata/clockface'
import RuleEndpointDropdown from 'src/alerting/components/notifications/RuleEndpointDropdown'
import RuleMessageContents from 'src/alerting/components/notifications/RuleMessageContents'

// Utils
import {getRuleVariantDefaults} from 'src/alerting/components/notifications/utils'
import {useRuleDispatch} from './RuleOverlayProvider'

// Types
import {NotificationEndpoint, NotificationRuleDraft, AppState} from 'src/types'

interface StateProps {
  endpoints: NotificationEndpoint[]
}

interface OwnProps {
  rule: NotificationRuleDraft
}

type Props = OwnProps & StateProps

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

const mstp = ({endpoints}: AppState) => {
  return {endpoints: endpoints.list}
}

export default connect(mstp)(RuleMessage)

// Libraries
import React, {FC, useEffect} from 'react'
import {connect} from 'react-redux'

// Components
import {Form, Panel, Grid, Columns} from '@influxdata/clockface'
import RuleEndpointDropdown from 'src/notifications/rules/components/RuleEndpointDropdown'
import RuleMessageContents from 'src/notifications/rules/components/RuleMessageContents'

// Utils
import {getRuleVariantDefaults} from 'src/notifications/rules/utils'
import {getAll} from 'src/resources/selectors'
import {useRuleDispatch} from './RuleOverlayProvider'

// Types
import {
  NotificationEndpoint,
  NotificationRuleDraft,
  AppState,
  ResourceType,
} from 'src/types'

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

  useEffect(() => {
    if (!rule.endpointID && endpoints.length) {
      onSelectEndpoint(endpoints[0].id)
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

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

const mstp = (state: AppState) => {
  const endpoints = getAll<NotificationEndpoint>(
    state,
    ResourceType.NotificationEndpoints
  )

  return {
    endpoints,
  }
}

export default connect(mstp)(RuleMessage)

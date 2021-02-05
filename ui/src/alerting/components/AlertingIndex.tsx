// Libraries
import React, {FunctionComponent, useState} from 'react'
import {Switch, Route} from 'react-router-dom'

//Components
import {Page, SelectGroup, ButtonShape} from '@influxdata/clockface'
import ChecksColumn from 'src/checks/components/ChecksColumn'
import RulesColumn from 'src/notifications/rules/components/RulesColumn'
import EndpointsColumn from 'src/notifications/endpoints/components/EndpointsColumn'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'
import GetResources from 'src/resources/components/GetResources'
import NewThresholdCheckEO from 'src/checks/components/NewThresholdCheckEO'
import NewDeadmanCheckEO from 'src/checks/components/NewDeadmanCheckEO'
import EditCheckEO from 'src/checks/components/EditCheckEO'
import NewRuleOverlay from 'src/notifications/rules/components/NewRuleOverlay'
import EditRuleOverlay from 'src/notifications/rules/components/EditRuleOverlay'
import NewEndpointOverlay from 'src/notifications/endpoints/components/NewEndpointOverlay'
import EditEndpointOverlay from 'src/notifications/endpoints/components/EditEndpointOverlay'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {ResourceType} from 'src/types'

const alertsPath = '/orgs/:orgID/alerting'

type ActiveColumn = 'checks' | 'endpoints' | 'rules'

const AlertingIndex: FunctionComponent = () => {
  const [activeColumn, setActiveColumn] = useState<ActiveColumn>('checks')

  const pageContentsClassName = `alerting-index alerting-index__${activeColumn}`

  const handleTabClick = (selectGroupOptionID: ActiveColumn): void => {
    setActiveColumn(selectGroupOptionID)
  }

  return (
    <>
      <Page titleTag={pageTitleSuffixer(['Alerts'])}>
        <Page.Header fullWidth={true} testID="alerts-page--header">
          <Page.Title title="Alerts" />
          <RateLimitAlert />
        </Page.Header>
        <Page.Contents
          fullWidth={true}
          scrollable={false}
          className={pageContentsClassName}
        >
          <GetResources resources={[ResourceType.Labels, ResourceType.Buckets]}>
            <GetAssetLimits>
              <SelectGroup
                className="alerting-index--selector"
                shape={ButtonShape.StretchToFit}
              >
                <SelectGroup.Option
                  value="checks"
                  id="checks"
                  onClick={handleTabClick}
                  name="alerting-active-tab"
                  active={activeColumn === 'checks'}
                  testID="alerting-tab--checks"
                  tabIndex={1}
                >
                  Checks
                </SelectGroup.Option>
                <SelectGroup.Option
                  value="endpoints"
                  id="endpoints"
                  onClick={handleTabClick}
                  name="alerting-active-tab"
                  active={activeColumn === 'endpoints'}
                  testID="alerting-tab--endpoints"
                  tabIndex={2}
                >
                  Notification Endpoints
                </SelectGroup.Option>
                <SelectGroup.Option
                  value="rules"
                  id="rules"
                  onClick={handleTabClick}
                  name="alerting-active-tab"
                  active={activeColumn === 'rules'}
                  testID="alerting-tab--rules"
                  tabIndex={3}
                >
                  Notification Rules
                </SelectGroup.Option>
              </SelectGroup>
              <div className="alerting-index--columns">
                <GetResources resources={[ResourceType.Checks]}>
                  <ChecksColumn tabIndex={1} />
                </GetResources>
                <GetResources resources={[ResourceType.NotificationEndpoints]}>
                  <EndpointsColumn tabIndex={2} />
                </GetResources>
                <GetResources resources={[ResourceType.NotificationRules]}>
                  <RulesColumn tabIndex={3} />
                </GetResources>
              </div>
            </GetAssetLimits>
          </GetResources>
        </Page.Contents>
      </Page>
      <Switch>
        <Route
          path={`${alertsPath}/checks/new-threshold`}
          component={NewThresholdCheckEO}
        />
        <Route
          path={`${alertsPath}/checks/new-deadman`}
          component={NewDeadmanCheckEO}
        />
        <Route
          path={`${alertsPath}/checks/:checkID/edit`}
          component={EditCheckEO}
        />
        <Route path={`${alertsPath}/rules/new`} component={NewRuleOverlay} />
        <Route
          path={`${alertsPath}/rules/:ruleID/edit`}
          component={EditRuleOverlay}
        />
        <Route
          path={`${alertsPath}/endpoints/new`}
          component={NewEndpointOverlay}
        />
        <Route
          path={`${alertsPath}/endpoints/:endpointID/edit`}
          component={EditEndpointOverlay}
        />
      </Switch>
    </>
  )
}

export default AlertingIndex

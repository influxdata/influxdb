// Libraries
import React, {FunctionComponent, useState} from 'react'
import {connect} from 'react-redux'

//Components
import {Page, SelectGroup, ButtonShape} from '@influxdata/clockface'
import ChecksColumn from 'src/checks/components/ChecksColumn'
import RulesColumn from 'src/notifications/rules/components/RulesColumn'
import EndpointsColumn from 'src/notifications/endpoints/components/EndpointsColumn'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import GetResources from 'src/resources/components/GetResources'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {
  extractMonitoringLimitStatus,
  extractLimitedMonitoringResources,
} from 'src/cloud/utils/limits'

// Types
import {AppState, ResourceType} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'

interface StateProps {
  limitStatus: LimitStatus
  limitedResources: string
}

type ActiveColumn = 'checks' | 'endpoints' | 'rules'

const AlertingIndex: FunctionComponent<StateProps> = ({
  children,
  limitStatus,
  limitedResources,
}) => {
  const [activeColumn, setActiveColumn] = useState<ActiveColumn>('checks')

  const pageContentsClassName = `alerting-index alerting-index__${activeColumn}`

  const handleTabClick = (selectGroupOptionID: ActiveColumn): void => {
    setActiveColumn(selectGroupOptionID)
  }

  return (
    <>
      <Page titleTag={pageTitleSuffixer(['Alerts'])}>
        <Page.Header fullWidth={true}>
          <Page.Title title="Alerts" />
          <CloudUpgradeButton />
        </Page.Header>
        <Page.Contents
          fullWidth={true}
          scrollable={false}
          className={pageContentsClassName}
        >
          <GetResources resources={[ResourceType.Labels, ResourceType.Buckets]}>
            <GetAssetLimits>
              <AssetLimitAlert
                resourceName={limitedResources}
                limitStatus={limitStatus}
                className="load-data--asset-alert"
              />
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
                >
                  Notification Rules
                </SelectGroup.Option>
              </SelectGroup>
              <div className="alerting-index--columns">
                <GetResources resources={[ResourceType.Checks]}>
                  <ChecksColumn />
                </GetResources>
                <GetResources resources={[ResourceType.NotificationEndpoints]}>
                  <EndpointsColumn />
                </GetResources>
                <GetResources resources={[ResourceType.NotificationRules]}>
                  <RulesColumn />
                </GetResources>
              </div>
            </GetAssetLimits>
          </GetResources>
        </Page.Contents>
      </Page>
      {children}
    </>
  )
}

const mstp = ({cloud: {limits}}: AppState): StateProps => {
  return {
    limitStatus: extractMonitoringLimitStatus(limits),
    limitedResources: extractLimitedMonitoringResources(limits),
  }
}

export default connect(mstp, null)(AlertingIndex)

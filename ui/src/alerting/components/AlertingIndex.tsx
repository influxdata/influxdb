// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

//Components
import {Grid, GridRow, GridColumn, Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import ChecksColumn from 'src/alerting/components/ChecksColumn'
import RulesColumn from 'src/alerting/components/notifications/RulesColumn'
import EndpointsColumn from 'src/alerting/components/EndpointsColumn'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import GetResources from 'src/shared/components/GetResources'

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

const AlertingIndex: FunctionComponent<StateProps> = ({
  children,
  limitStatus,
  limitedResources,
}) => {
  return (
    <>
      <Page titleTag={pageTitleSuffixer(['Monitoring & Alerting'])}>
        <Page.Header fullWidth={false}>
          <Page.HeaderLeft>
            <PageTitleWithOrg title="Monitoring & Alerting" />
          </Page.HeaderLeft>
          <Page.HeaderRight />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={false}>
          <GetResources resources={[ResourceType.Labels]}>
            <GetAssetLimits>
              <AssetLimitAlert
                resourceName={limitedResources}
                limitStatus={limitStatus}
                className="load-data--asset-alert"
              />
              <Grid className="alerting-index">
                <GridRow testID="grid--row">
                  <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                    <GetResources resources={[ResourceType.Checks]}>
                      <ChecksColumn />
                    </GetResources>
                  </GridColumn>
                  <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                    <GetResources
                      resources={[ResourceType.NotificationEndpoints]}
                    >
                      <EndpointsColumn />
                    </GetResources>
                  </GridColumn>
                  <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                    <GetResources resources={[ResourceType.NotificationRules]}>
                      <RulesColumn />
                    </GetResources>
                  </GridColumn>
                </GridRow>
              </Grid>
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

export default connect(
  mstp,
  null
)(AlertingIndex)

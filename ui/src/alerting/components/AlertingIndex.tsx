// Libraries
import React, {FunctionComponent} from 'react'

//Components
import {Grid, GridRow, GridColumn, Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import ChecksColumn from 'src/alerting/components/ChecksColumn'
import RulesColumn from 'src/alerting/components/notifications/RulesColumn'
import EndpointsColumn from 'src/alerting/components/EndpointsColumn'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

const AlertingIndex: FunctionComponent = ({children}) => {
  return (
    <>
      <Page titleTag={pageTitleSuffixer(['Monitoring & Alerting'])}>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <PageTitleWithOrg title="Monitoring & Alerting" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={false}>
          <GetResources resource={ResourceTypes.Labels}>
            <Grid className="alerting-index">
              <GridRow testID="grid--row">
                <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                  <GetResources resource={ResourceTypes.Checks}>
                    <ChecksColumn />
                  </GetResources>
                </GridColumn>
                <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                  <GetResources resource={ResourceTypes.NotificationRules}>
                    <RulesColumn />
                  </GetResources>
                </GridColumn>
                <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                  <GetResources resource={ResourceTypes.NotificationEndpoints}>
                    <EndpointsColumn />
                  </GetResources>
                </GridColumn>
              </GridRow>
            </Grid>
          </GetResources>
        </Page.Contents>
      </Page>
      {children}
    </>
  )
}

export default AlertingIndex

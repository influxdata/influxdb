// Libraries
import React, {FunctionComponent} from 'react'

//Components
import {Grid, GridRow, GridColumn} from '@influxdata/clockface'
import {Page} from 'src/pageLayout'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import ChecksColumn from 'src/alerting/components/ChecksColumn'
import RulesColumn from 'src/alerting/components/notifications/RulesColumn'
import EndpointsColumn from 'src/alerting/components/EndpointsColumn'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

const AlertingIndex: FunctionComponent = ({children}) => {
  return (
    <>
      <Page titleTag="Monitoring & Alerting" className="alerting-index">
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <PageTitleWithOrg title="Monitoring & Alerting" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={false}>
          <div className="col-xs-12">
            <GetResources resource={ResourceTypes.Labels}>
              <Grid>
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
                    <GetResources
                      resource={ResourceTypes.NotificationEndpoints}
                    >
                      <EndpointsColumn />
                    </GetResources>
                  </GridColumn>
                </GridRow>
              </Grid>
            </GetResources>
          </div>
        </Page.Contents>
      </Page>
      {children}
    </>
  )
}

export default AlertingIndex

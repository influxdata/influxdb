// Libraries
import React, {FunctionComponent} from 'react'

//Components
import {Page, Grid, GridRow, GridColumn} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import AlertChecksColumn from 'src/alerting/components/AlertChecksColumn'
import NotificationRulesColumn from 'src/alerting/components/NotificationRulesColumn'
import EndpointsColumn from 'src/alerting/components/EndpointsColumn'

const AlertingIndex: FunctionComponent = ({children}) => {
  return (
    <>
      <Page titleTag="Alerting">
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <PageTitleWithOrg title="Alerting" />
          </Page.Header.Left>
        </Page.Header>
        <Page.Contents fullWidth={true} scrollable={true}>
          <Grid>
            <GridRow testID="grid--row">
              <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                <AlertChecksColumn />
              </GridColumn>
              <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                <NotificationRulesColumn />
              </GridColumn>
              <GridColumn widthLG={4} widthMD={4} widthSM={4} widthXS={12}>
                <EndpointsColumn />
              </GridColumn>
            </GridRow>
          </Grid>
        </Page.Contents>
      </Page>

      {children}
    </>
  )
}

export default AlertingIndex

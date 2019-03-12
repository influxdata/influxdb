// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import Dashboards from 'src/organizations/components/Dashboards'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

//Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'

// APIs
import {getDashboards} from 'src/organizations/apis'

// Types
import {Organization} from '@influxdata/influx'
import {AppState, Dashboard} from 'src/types/v2'

interface RouterProps {
  params: {
    orgID: string
  }
}

interface DispatchProps {
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & RouterProps & DispatchProps & StateProps

@ErrorHandling
class OrgDashboardsIndex extends Component<Props> {
  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <OrgHeader orgID={org.id} />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <OrganizationNavigation tab={'dashboards'} orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--dashboards"
                    url="dashboards"
                    title="Dashboards"
                  >
                    <GetOrgResources<Dashboard>
                      organization={org}
                      fetcher={getDashboards}
                    >
                      {(dashboards, loading, fetch) => (
                        <SpinnerContainer
                          loading={loading}
                          spinnerComponent={<TechnoSpinner />}
                        >
                          <Dashboards
                            dashboards={dashboards}
                            orgName={org.name}
                            onChange={fetch}
                            orgID={org.id}
                          />
                        </SpinnerContainer>
                      )}
                    </GetOrgResources>
                  </TabbedPageSection>
                </Tabs.TabContents>
              </Tabs>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }
}

const mstp = (state: AppState, props: Props) => {
  const {orgs} = state
  const org = orgs.find(o => o.id === props.params.orgID)
  return {
    org,
  }
}

const mdtp: DispatchProps = {
  notify: notifyActions.notify,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(OrgDashboardsIndex))

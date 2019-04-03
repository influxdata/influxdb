// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import {Tabs} from 'src/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import GetOrgResources from 'src/organizations/components/GetOrgResources'
import Scrapers from 'src/organizations/components/Scrapers'

// APIs
import {client} from 'src/utils/api'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'

// Types
import {Organization, ScraperTargetResponse, Bucket} from '@influxdata/influx'
import {AppState} from 'src/types'

const getScrapers = async (): Promise<ScraperTargetResponse[]> => {
  return await client.scrapers.getAll()
}

const getBuckets = async (org: Organization): Promise<Bucket[]> => {
  return client.buckets.getAll(org.id)
}

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
class OrgScrapersIndex extends Component<Props> {
  public render() {
    const {org, notify} = this.props

    return (
      <Page titleTag={org.name}>
        <OrgHeader />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <OrganizationNavigation tab="scrapers" orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="org-view-tab--scrapers"
                  url="scrapers"
                  title="Scrapers"
                >
                  <GetOrgResources<ScraperTargetResponse>
                    organization={org}
                    fetcher={getScrapers}
                  >
                    {(scrapers, loading, fetch) => {
                      return (
                        <SpinnerContainer
                          loading={loading}
                          spinnerComponent={<TechnoSpinner />}
                        >
                          <GetOrgResources<Bucket>
                            organization={org}
                            fetcher={getBuckets}
                          >
                            {(buckets, loading) => (
                              <SpinnerContainer
                                loading={loading}
                                spinnerComponent={<TechnoSpinner />}
                              >
                                <Scrapers
                                  scrapers={scrapers}
                                  onChange={fetch}
                                  orgName={org.name}
                                  buckets={buckets}
                                  notify={notify}
                                />
                              </SpinnerContainer>
                            )}
                          </GetOrgResources>
                        </SpinnerContainer>
                      )
                    }}
                  </GetOrgResources>
                </TabbedPageSection>
              </Tabs.TabContents>
            </Tabs>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = (state: AppState, props: Props) => {
  const {
    orgs: {items},
  } = state
  const org = items.find(o => o.id === props.params.orgID)
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
)(withRouter<{}>(OrgScrapersIndex))

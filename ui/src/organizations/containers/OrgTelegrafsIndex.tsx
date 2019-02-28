// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {AppState} from 'src/types/v2'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import Collectors from 'src/organizations/components/Collectors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'

// Types
import {Bucket, Organization, Telegraf} from '@influxdata/influx'
import {client} from 'src/utils/api'

const getBuckets = async (org: Organization) => {
  return client.buckets.getAllByOrg(org.name)
}
const getCollectors = async (org: Organization) => {
  return client.telegrafConfigs.getAllByOrg(org)
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
class OrgTelegrafsIndex extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {org, notify} = this.props

    return (
      <Page titleTag={org.name}>
        <OrgHeader orgID={org.id} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <OrganizationNavigation tab={'telegrafs'} orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="org-view-tab--telegrafs"
                  url="telegrafs"
                  title="Telegraf"
                >
                  <GetOrgResources<Telegraf>
                    organization={org}
                    fetcher={getCollectors}
                  >
                    {(collectors, loading, fetch) => (
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
                              <Collectors
                                collectors={collectors}
                                onChange={fetch}
                                notify={notify}
                                buckets={buckets}
                                orgName={org.name}
                              />
                            </SpinnerContainer>
                          )}
                        </GetOrgResources>
                      </SpinnerContainer>
                    )}
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

const mstp = (state: AppState, props: WithRouterProps) => {
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
)(withRouter<{}>(OrgTelegrafsIndex))

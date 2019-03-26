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
import GetOrgResources from 'src/organizations/components/GetOrgResources'
import Members from 'src/organizations/components/Members'

//Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'

// APIs
import {client} from 'src/utils/api'

// Types
import {ResourceOwner} from '@influxdata/influx'
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types'

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

type Props = WithRouterProps & RouterProps & StateProps & DispatchProps

const getOwnersAndMembers = async (org: Organization) => {
  const allMembers = await Promise.all([
    client.organizations.owners(org.id),
    client.organizations.members(org.id),
  ])

  return [].concat(...allMembers)
}

@ErrorHandling
class OrgMembersIndex extends Component<Props> {
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
              <OrganizationNavigation tab="members" orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="org-view-tab--members"
                  url="members"
                  title="Members"
                >
                  <GetOrgResources<ResourceOwner>
                    organization={org}
                    fetcher={getOwnersAndMembers}
                  >
                    {(members, loading, fetch) => (
                      <SpinnerContainer
                        loading={loading}
                        spinnerComponent={<TechnoSpinner />}
                      >
                        <Members
                          members={members}
                          orgName={org.name}
                          orgID={org.id}
                          notify={notify}
                          onChange={fetch}
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

export default connect<StateProps>(
  mstp,
  mdtp
)(withRouter<{}>(OrgMembersIndex))

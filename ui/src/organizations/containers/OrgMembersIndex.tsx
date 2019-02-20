import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {AppState} from 'src/types/v2'
import {ResourceOwner} from '@influxdata/influx'

// Components
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import Members from 'src/organizations/components/Members'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Organization} from '@influxdata/influx'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

import {client} from 'src/utils/api'

interface RouterProps {
  params: {
    orgID: string
  }
}

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & RouterProps & StateProps

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
    const {org} = this.props

    return (
      <Page titleTag={org.name}>
        <OrgHeader orgID={org.id} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <OrganizationNavigation tab={'members_tab'} orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="org-view-tab--members"
                  url="members_tab"
                  title="Members"
                >
                  <GetOrgResources<ResourceOwner>
                    organization={org}
                    fetcher={getOwnersAndMembers}
                  >
                    {(members, loading) => (
                      <SpinnerContainer
                        loading={loading}
                        spinnerComponent={<TechnoSpinner />}
                      >
                        <Members members={members} orgName={org.name} />
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

export default connect<StateProps>(mstp)(withRouter<{}>(OrgMembersIndex))

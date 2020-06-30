import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrgTabbedPage from 'src/organizations/components/OrgTabbedPage'
import OrgHeader from 'src/organizations/components/OrgHeader'
import {Page} from '@influxdata/clockface'
import GetResources from 'src/resources/components/GetResources'
import Members from 'src/members/components/Members'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getByID} from 'src/resources/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & StateProps

@ErrorHandling
class MembersIndex extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Members', 'Organization'])}>
          <OrgHeader />
          <OrgTabbedPage activeTab="members" orgID={org.id}>
            <GetResources resources={[ResourceType.Members]}>
              <Members />
            </GetResources>
          </OrgTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState, props: Props) => {
  const org = getByID<Organization>(
    state,
    ResourceType.Orgs,
    props.params.orgID
  )

  return {
    org,
  }
}

export default connect<StateProps>(mstp, null)(withRouter<{}>(MembersIndex))

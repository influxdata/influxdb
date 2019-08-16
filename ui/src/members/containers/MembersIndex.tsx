import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from 'src/pageLayout'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import Members from 'src/members/components/Members'

// Types
import {AppState, Organization} from 'src/types'

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
        <Page titleTag={org.name}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="members" orgID={org.id}>
            <GetResources resource={ResourceTypes.Members}>
              <Members />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = ({orgs: {items}}: AppState, props: Props) => {
  const org = items.find(o => o.id === props.params.orgID)
  return {
    org,
  }
}

export default connect<StateProps>(
  mstp,
  null
)(withRouter<{}>(MembersIndex))

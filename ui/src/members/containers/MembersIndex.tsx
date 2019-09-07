import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from '@influxdata/clockface'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'
import Members from 'src/members/components/Members'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

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
        <Page titleTag={pageTitleSuffixer(['Members', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="members" orgID={org.id}>
            <GetResources resource={ResourceType.Members}>
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

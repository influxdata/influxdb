import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsNavigation from 'src/settings/components/SettingsNavigation'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import Members from 'src/members/components/Members'

//Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'

// Types
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types'

interface DispatchProps {
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & StateProps & DispatchProps

@ErrorHandling
class MembersIndex extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {org} = this.props

    return (
      <Page titleTag={org.name}>
        <SettingsHeader />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <SettingsNavigation tab="members" orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="settings-tab--members"
                  url="members"
                  title="Members"
                >
                  <GetResources resource={ResourceTypes.Members}>
                    <Members />
                    {this.props.children}
                  </GetResources>
                </TabbedPageSection>
              </Tabs.TabContents>
            </Tabs>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = ({orgs: {items}}: AppState, props: Props) => {
  const org = items.find(o => o.id === props.params.orgID)
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
)(withRouter<{}>(MembersIndex))

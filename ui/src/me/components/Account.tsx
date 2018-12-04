// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps} from 'react-router'

// Components
import {Page} from 'src/pageLayout'
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import ProfilePageSection from 'src/shared/components/profile_page/ProfilePageSection'

export default class Account extends PureComponent<WithRouterProps> {
  public render() {
    const {params} = this.props
    return (
      <Page>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title="Your name here" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <ProfilePage
            name="Account"
            activeTabUrl={params.tab}
            parentUrl="/account"
          >
            <ProfilePageSection id="settings" title="Settings" url="settings">
              <div>Settings go here</div>
            </ProfilePageSection>
            <ProfilePageSection id="tokens" title="Tokens" url="tokens">
              <div>Tokens go here</div>
            </ProfilePageSection>
          </ProfilePage>
        </Page.Contents>
      </Page>
    )
  }
}

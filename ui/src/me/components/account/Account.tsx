// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps} from 'react-router'

// Components
import {Page} from 'src/pageLayout'
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import ProfilePageSection from 'src/shared/components/profile_page/ProfilePageSection'
import Settings from 'src/me/components/account/Settings'
import Tokens from 'src/me/components/account/Tokens'

export enum Tabs {
  Settings = 'settings',
  Tokens = 'tokens',
}

export default class Account extends PureComponent<WithRouterProps> {
  public render() {
    const {params} = this.props
    return (
      <Page>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title="My Account" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <ProfilePage
            name="Account"
            activeTabUrl={params.tab}
            parentUrl="/account"
          >
            <ProfilePageSection
              title="Settings"
              id={Tabs.Settings}
              url={Tabs.Settings}
            >
              <Settings />
            </ProfilePageSection>
            <ProfilePageSection
              title="Tokens"
              id={Tabs.Tokens}
              url={Tabs.Tokens}
            >
              <Tokens />
            </ProfilePageSection>
          </ProfilePage>
        </Page.Contents>
      </Page>
    )
  }
}

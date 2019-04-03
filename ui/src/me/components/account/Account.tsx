// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps} from 'react-router'

// Components
import {Page} from 'src/pageLayout'
import TabbedPage from 'src/shared/components/tabbed_page/TabbedPage'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import Settings from 'src/me/components/account/Settings'

export enum Tabs {
  Settings = 'settings',
  Tokens = 'tokens',
}

export default class Account extends PureComponent<WithRouterProps> {
  public render() {
    const {params} = this.props
    return (
      <Page titleTag="Account">
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title="Account" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <TabbedPage
            name="Account"
            activeTabUrl={params.tab}
            parentUrl="/account"
          >
            <TabbedPageSection
              title="My Settings"
              id={Tabs.Settings}
              url={Tabs.Settings}
            >
              <Settings />
            </TabbedPageSection>
          </TabbedPage>
        </Page.Contents>
      </Page>
    )
  }
}

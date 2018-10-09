// Libraries
import React, {PureComponent} from 'react'

/// Components
import {Page} from 'src/pageLayout'
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import UserSettings from 'src/user/components/UserSettings'
import TokenManager from 'src/user/components/TokenManager'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// MOCK DATA
import {LeroyJenkins} from 'src/user/mockUserData'

interface UserToken {
  id: string
  name: string
  secretKey: string
}

interface User {
  id: string
  name: string
  email: string
  tokens: UserToken[]
  avatar: string
}

interface Props {
  user?: User
  params: {
    tab: string
  }
}

@ErrorHandling
export class UserPage extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    user: LeroyJenkins,
  }

  public render() {
    const {user, params} = this.props

    return (
      <Page>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title="My Profile" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <ProfilePage
              name={user.name}
              avatar={user.avatar}
              parentUrl="/user"
              activeTabUrl={params.tab}
            >
              <ProfilePage.Section
                id="user-profile-tab--settings"
                url="settings"
                title="Settings"
              >
                <UserSettings blargh="User Settings" />
              </ProfilePage.Section>
              <ProfilePage.Section
                id="user-profile-tab--tokens"
                url="tokens"
                title="Tokens"
              >
                <TokenManager token="Token Manager" />
              </ProfilePage.Section>
            </ProfilePage>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

export default UserPage

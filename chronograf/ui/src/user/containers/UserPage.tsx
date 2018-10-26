// Libraries
import React, {PureComponent} from 'react'

// Styles
import 'src/user/containers/UserPage.scss'

// Components
import {Page} from 'src/pageLayout'
import Resources from 'src/user/components/Resources'
import Header from 'src/user/components/UserPageHeader'
import Docs from 'src/user/components/Docs'

// Types
import {User} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// MOCK DATA
import {LeroyJenkins} from 'src/user/mockUserData'
import {Panel} from 'src/clockface'

interface Props {
  user: User
}

@ErrorHandling
export class UserPage extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    user: LeroyJenkins,
  }

  public render() {
    const {user} = this.props

    return (
      <Page className="user-page">
        <Header title={`Howdy, ${user.name}!`} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-8">
            <Panel>
              <Panel.Header title="Getting Started" />
              <Panel.Body>
                <span>Put Getting Started Stuff Here</span>
              </Panel.Body>
            </Panel>
            <Docs />
          </div>
          <div className="col-xs-4">
            <Resources user={user} />
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

export default UserPage

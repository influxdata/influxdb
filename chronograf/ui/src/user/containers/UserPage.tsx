// Libraries
import React, {PureComponent} from 'react'

/// Components
import {Page} from 'src/page_layout'

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
}

@ErrorHandling
export class FluxPage extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    user: LeroyJenkins,
  }

  public render() {
    return (
      <Page>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title="My Profile" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <div>fsfsfsdfs</div>
        </Page.Contents>
      </Page>
    )
  }
}

export default FluxPage

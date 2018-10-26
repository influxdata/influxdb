// Libraries
import React, {PureComponent} from 'react'

// Components
import {Button} from 'src/clockface'

import {logout} from 'src/user/apis'

interface Props {
  signOutLink: string
}

export default class Settings extends PureComponent<Props> {
  public render() {
    return <Button onClick={this.handleSignOut} text="Sign Out" />
  }

  private handleSignOut = async () => {
    await logout(this.props.signOutLink)
    window.location.reload()
  }
}

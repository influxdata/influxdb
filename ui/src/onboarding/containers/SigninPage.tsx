// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SplashPage from 'src/shared/components/splash_page/SplashPage'

import {
  Button,
  ComponentColor,
  ComponentSize,
  Input,
  InputType,
  Form,
  Columns,
} from 'src/clockface'

// APIs
import {signin} from 'src/onboarding/apis'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {Links} from 'src/types/v2/links'
import {Notification, NotificationFunc} from 'src/types'

export interface Props {
  links: Links
  notify: (message: Notification | NotificationFunc) => void
  onSignInUser: () => void
}

interface State {
  username: string
  password: string
}

@ErrorHandling
class SigninPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      username: '',
      password: '',
    }
  }

  public render() {
    const {username, password} = this.state
    return (
      <SplashPage panelWidthPixels={300}>
        <SplashPage.Panel>
          <SplashPage.Logo />
          <SplashPage.Header title="InfluxData" />
          <Form>
            <Form.Element
              label="Username"
              colsXS={Columns.Twelve}
              errorMessage={''}
            >
              <Input
                value={username}
                onChange={this.handleUsername}
                size={ComponentSize.Medium}
              />
            </Form.Element>
            <Form.Element
              label="Password"
              colsXS={Columns.Twelve}
              errorMessage={''}
            >
              <Input
                value={password}
                onChange={this.handlePassword}
                size={ComponentSize.Medium}
                type={InputType.Password}
              />
            </Form.Element>
            <Form.Footer>
              <Button
                color={ComponentColor.Primary}
                text="Sign In"
                size={ComponentSize.Medium}
                onClick={this.handleSignIn}
              />
            </Form.Footer>
          </Form>
        </SplashPage.Panel>
      </SplashPage>
    )
  }

  private handleUsername = (e: ChangeEvent<HTMLInputElement>): void => {
    const username = e.target.value
    this.setState({username})
  }
  private handlePassword = (e: ChangeEvent<HTMLInputElement>): void => {
    const password = e.target.value
    this.setState({password})
  }

  private handleSignIn = async (): Promise<void> => {
    const {links, notify, onSignInUser} = this.props
    const {username, password} = this.state
    try {
      await signin(links.signin, {username, password})
      onSignInUser()
    } catch (error) {
      notify(copy.SigninError)
    }
  }
}

const mstp = ({links}) => ({
  links,
})

const mdtp = {
  notify: notifyAction,
}

export default connect(mstp, mdtp)(SigninPage)

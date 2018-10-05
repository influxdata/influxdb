// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  Input,
  Form,
  Columns,
  WizardFullScreen,
  InputType,
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
      <WizardFullScreen>
        <div className="wizard-step--container">
          <div className="onboarding-step">
            <h3 className="wizard-step-title">Please sign in</h3>
            <Form>
              <Form.Element
                label="Username"
                colsXS={Columns.Six}
                offsetXS={Columns.Three}
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
                colsXS={Columns.Six}
                offsetXS={Columns.Three}
                errorMessage={''}
              >
                <Input
                  value={password}
                  type={InputType.Password}
                  onChange={this.handlePassword}
                  size={ComponentSize.Medium}
                />
              </Form.Element>
            </Form>
            <Button
              color={ComponentColor.Primary}
              text="Sign In"
              size={ComponentSize.Medium}
              onClick={this.handleSignIn}
            />
          </div>
        </div>
      </WizardFullScreen>
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

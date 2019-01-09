// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import _, {get} from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  Button,
  ComponentColor,
  ComponentSize,
  Input,
  InputType,
  Form,
  Grid,
  Columns,
  ButtonType,
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
class SigninForm extends PureComponent<Props, State> {
  public state: State = {
    username: '',
    password: '',
  }

  public render() {
    const {username, password} = this.state
    return (
      <Form onSubmit={this.handleSignIn}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Username">
                <Input
                  value={username}
                  onChange={this.handleUsername}
                  size={ComponentSize.Medium}
                  autoFocus={true}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Password">
                <Input
                  value={password}
                  onChange={this.handlePassword}
                  size={ComponentSize.Medium}
                  type={InputType.Password}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Footer>
                <Button
                  color={ComponentColor.Primary}
                  text="Sign In"
                  size={ComponentSize.Medium}
                  type={ButtonType.Submit}
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
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
    const {notify, onSignInUser} = this.props
    const {username, password} = this.state
    try {
      await signin({username, password})
      onSignInUser()
    } catch (error) {
      const message = get(error, 'data.msg', '')

      if (!message) {
        return notify(copy.SigninError)
      }

      notify({...copy.SigninError, message})
    }
  }
}

const mstp = ({links}) => ({
  links,
})

const mdtp = {
  notify: notifyAction,
}

export default connect(
  mstp,
  mdtp
)(SigninForm)

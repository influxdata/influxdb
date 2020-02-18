import React, {FC, useState, ChangeEvent} from 'react'
import {
  Button,
  ButtonShape,
  ButtonType,
  Columns,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  Form,
  Grid,
  Input,
  InputType,
  VisibilityInput,
} from '@influxdata/clockface'

interface Props {
  buttonStatus: ComponentStatus
  emailError?: string
  email: string
  passwordError?: string
  password: string
  handleInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  handleForgotPasswordClick: () => void
}

const LoginForm: FC<Props> = ({
  buttonStatus,
  emailError,
  email,
  passwordError,
  password,
  handleInputChange,
  handleForgotPasswordClick,
}) => {
  const [visible, toggleIcon] = useState(false)
  return (
    <>
      <Grid>
        <Grid.Row className="sign-up--form-padded-row">
          <Grid.Column widthXS={Columns.Twelve}>
            <Form.Element
              label="Work Email Address"
              required={true}
              errorMessage={emailError}
            >
              <Input
                name="email"
                value={email}
                // type={InputType.Email}
                size={ComponentSize.Large}
                status={
                  emailError ? ComponentStatus.Error : ComponentStatus.Default
                }
                onChange={handleInputChange}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Form.Element
              label="Password"
              required={true}
              errorMessage={passwordError}
            >
              <VisibilityInput
                name="password"
                value={password}
                size={ComponentSize.Large}
                onChange={handleInputChange}
                visible={visible}
                status={
                  passwordError
                    ? ComponentStatus.Error
                    : ComponentStatus.Default
                }
                onToggleClick={() => toggleIcon(!visible)}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <a onClick={handleForgotPasswordClick} className="login--forgot-password">
        Forgot Password?
      </a>
      <Button
        className="create-account--button"
        text="Login"
        color={ComponentColor.Primary}
        size={ComponentSize.Large}
        type={ButtonType.Submit}
        status={buttonStatus}
        shape={ButtonShape.StretchToFit}
      />
    </>
  )
}

export default LoginForm

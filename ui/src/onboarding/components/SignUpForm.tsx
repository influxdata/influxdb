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
  confirmPasswordError?: string
  confirmPassword: string
  emailError?: string
  email: string
  firstName: string
  firstNameError?: string
  lastName: string
  lastNameError?: string
  passwordError?: string
  password: string
  handleInputChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const SignUpForm: FC<Props> = ({
  buttonStatus,
  confirmPasswordError,
  confirmPassword,
  emailError,
  email,
  firstName,
  firstNameError,
  lastName,
  lastNameError,
  passwordError,
  password,
  handleInputChange,
}) => {
  const [visible, toggleIcon] = useState(false)
  return (
    <>
      <Grid>
        <Grid.Row className="sign-up--form-padded-row">
          <Grid.Column widthXS={Columns.Six}>
            <Form.Element
              label="First Name"
              required={true}
              errorMessage={firstNameError}
            >
              <Input
                name="firstName"
                value={firstName}
                autoFocus={true}
                size={ComponentSize.Large}
                status={
                  firstNameError
                    ? ComponentStatus.Error
                    : ComponentStatus.Default
                }
                onChange={handleInputChange}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Six}>
            <Form.Element
              label="Last Name"
              required={true}
              errorMessage={lastNameError}
            >
              <Input
                name="lastName"
                value={lastName}
                size={ComponentSize.Large}
                status={
                  lastNameError
                    ? ComponentStatus.Error
                    : ComponentStatus.Default
                }
                onChange={handleInputChange}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Form.Element
              label="Work Email Address"
              required={true}
              errorMessage={emailError}
            >
              <Input
                name="email"
                value={email}
                type={InputType.Email}
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
          <Grid.Column widthXS={Columns.Twelve}>
            <Form.Element
              label="Confirm Password"
              required={true}
              errorMessage={confirmPasswordError}
            >
              <VisibilityInput
                name="confirmPassword"
                value={confirmPassword}
                size={ComponentSize.Large}
                onChange={handleInputChange}
                visible={visible}
                status={
                  confirmPasswordError
                    ? ComponentStatus.Error
                    : ComponentStatus.Default
                }
                onToggleClick={() => toggleIcon(!visible)}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <Button
        className="create-account--button"
        text="Create Account"
        color={ComponentColor.Primary}
        size={ComponentSize.Large}
        type={ButtonType.Submit}
        status={buttonStatus}
        shape={ButtonShape.StretchToFit}
      />
    </>
  )
}

export default SignUpForm

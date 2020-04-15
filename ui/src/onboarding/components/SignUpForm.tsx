// Libraries
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

// Types
import {FormFieldValidation} from 'src/types'

interface Props {
  buttonStatus: ComponentStatus
  confirmPassword: string
  confirmPasswordValidation: FormFieldValidation
  email: string
  emailValidation: FormFieldValidation
  firstName: string
  firstNameValidation: FormFieldValidation
  lastName: string
  lastNameValidation: FormFieldValidation
  password: string
  passwordValidation: FormFieldValidation
  handleInputChange: (event: ChangeEvent<HTMLInputElement>) => void
}

export const SignUpForm: FC<Props> = ({
  buttonStatus,
  confirmPassword,
  confirmPasswordValidation,
  email,
  emailValidation,
  firstName,
  firstNameValidation,
  lastName,
  lastNameValidation,
  password,
  passwordValidation,
  handleInputChange,
}) => {
  const [isVisible, toggleVisibility] = useState(false)
  return (
    <>
      <Grid>
        <Grid.Row className="sign-up--form-padded-row">
          <Grid.Column widthXS={Columns.Six}>
            <Form.Element
              label="First Name"
              required={true}
              errorMessage={firstNameValidation.errorMessage}
            >
              <Input
                name="firstName"
                value={firstName}
                autoFocus={true}
                size={ComponentSize.Large}
                status={
                  firstNameValidation.hasError
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
              errorMessage={lastNameValidation.errorMessage}
            >
              <Input
                name="lastName"
                value={lastName}
                size={ComponentSize.Large}
                status={
                  lastNameValidation.hasError
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
              errorMessage={emailValidation.errorMessage}
            >
              <Input
                name="email"
                value={email}
                type={InputType.Email}
                size={ComponentSize.Large}
                status={
                  emailValidation.hasError
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
              label="Password"
              required={true}
              errorMessage={passwordValidation.errorMessage}
            >
              <VisibilityInput
                name="password"
                value={password}
                size={ComponentSize.Large}
                onChange={handleInputChange}
                visible={isVisible}
                status={
                  passwordValidation.hasError
                    ? ComponentStatus.Error
                    : ComponentStatus.Default
                }
                onToggleClick={() => toggleVisibility(!isVisible)}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Twelve}>
            <Form.Element
              label="Confirm Password"
              required={true}
              errorMessage={confirmPasswordValidation.errorMessage}
            >
              <VisibilityInput
                name="confirmPassword"
                value={confirmPassword}
                size={ComponentSize.Large}
                onChange={handleInputChange}
                visible={isVisible}
                status={
                  confirmPasswordValidation.hasError
                    ? ComponentStatus.Error
                    : ComponentStatus.Default
                }
                onToggleClick={() => toggleVisibility(!isVisible)}
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

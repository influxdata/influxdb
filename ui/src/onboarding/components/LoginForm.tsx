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
  FontWeight,
  Form,
  Grid,
  Heading,
  HeadingElement,
  Input,
  InputType,
  Typeface,
  VisibilityInput,
} from '@influxdata/clockface'

// Types
import {FormFieldValidation} from 'src/types'

interface Props {
  buttonStatus: ComponentStatus
  emailValidation: FormFieldValidation
  email: string
  passwordValidation: FormFieldValidation
  password: string
  handleInputChange: (event: ChangeEvent<HTMLInputElement>) => void
  handleForgotPasswordClick: () => void
}

export const LoginForm: FC<Props> = ({
  buttonStatus,
  emailValidation,
  email,
  passwordValidation,
  password,
  handleInputChange,
  handleForgotPasswordClick,
}) => {
  const [isVisible, toggleVisibility] = useState(false)
  return (
    <>
      <Grid>
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
        </Grid.Row>
      </Grid>
      <Heading
        element={HeadingElement.H6}
        type={Typeface.Rubik}
        weight={FontWeight.Regular}
        onClick={handleForgotPasswordClick}
        className="login--forgot-password"
        selectable={true}
      >
        Forgot Password?
      </Heading>
      <Button
        className="create-account--button"
        text="Log In"
        color={ComponentColor.Primary}
        size={ComponentSize.Large}
        type={ButtonType.Submit}
        status={buttonStatus}
        shape={ButtonShape.StretchToFit}
      />
    </>
  )
}

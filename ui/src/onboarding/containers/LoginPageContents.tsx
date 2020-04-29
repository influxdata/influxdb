// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import {connect} from 'react-redux'
import {
  AlignItems,
  ComponentSize,
  ComponentStatus,
  FlexBox,
  FlexDirection,
  FontWeight,
  Grid,
  Heading,
  HeadingElement,
  JustifyContent,
  Method,
  Panel,
  Typeface,
} from '@influxdata/clockface'
import auth0js, {WebAuth} from 'auth0-js'

// Components
import {LoginForm} from 'src/onboarding/components/LoginForm'
import {SocialButton} from 'src/shared/components/SocialButton'
import {GoogleLogo} from 'src/clientLibraries/graphics'

// Types
import {Auth0Connection, FormFieldValidation} from 'src/types'

// APIs & Actions
import {notify} from 'src/shared/actions/notifications'
import {passwordResetSuccessfully} from 'src/shared/copy/notifications'
import {getAuth0Config} from 'src/authorizations/apis'

interface ErrorObject {
  emailError?: string
  passwordError?: string
}

interface DispatchProps {
  onNotify: typeof notify
}

interface State {
  buttonStatus: ComponentStatus
  email: string
  emailError: string
  password: string
  passwordError: string
}

class LoginPageContents extends PureComponent<DispatchProps> {
  private auth0: typeof WebAuth

  state: State = {
    buttonStatus: ComponentStatus.Default,
    email: '',
    emailError: '',
    password: '',
    passwordError: '',
  }

  public async componentDidMount() {
    try {
      const config = await getAuth0Config()
      this.auth0 = new auth0js.WebAuth({
        domain: config.domain,
        clientID: config.clientID,
        redirectUri: config.redirectURL,
        responseType: 'code',
        state: config.state,
      })
    } catch (error) {
      console.error(error)
      // TODO: uncomment after demo day
      // redirect to universal login page if there's an error
      // window.location.href =
      // 'https://auth.a.influxcloud.net/'
      throw error
    }
  }

  render() {
    const {
      buttonStatus,
      email,
      emailError,
      password,
      passwordError,
    } = this.state

    return (
      <form
        action="/signup"
        method={Method.Post}
        onSubmit={this.handleSubmit}
        className="sign-up--form"
      >
        <Panel className="sign-up--form-panel">
          <Panel.Header
            size={ComponentSize.Large}
            justifyContent={JustifyContent.Center}
          >
            <Heading
              element={HeadingElement.H5}
              type={Typeface.Rubik}
              weight={FontWeight.Regular}
              className="heading--margins"
            >
              Continue with
            </Heading>
          </Panel.Header>
          <Panel.Body size={ComponentSize.Large}>
            <Grid>
              <Grid.Row className="sign-up--social-button-group">
                <FlexBox
                  stretchToFitWidth={true}
                  direction={FlexDirection.Column}
                  justifyContent={JustifyContent.Center}
                  alignItems={AlignItems.Center}
                  margin={ComponentSize.Large}
                >
                  <SocialButton
                    handleClick={() => {
                      this.handleSocialClick(Auth0Connection.Google)
                    }}
                    buttonText="Google"
                  >
                    <GoogleLogo className="signup-icon" />
                  </SocialButton>
                </FlexBox>
              </Grid.Row>
            </Grid>
            <Heading
              element={HeadingElement.H5}
              type={Typeface.Rubik}
              weight={FontWeight.Regular}
              className="heading--margins"
            >
              OR
            </Heading>
            <LoginForm
              buttonStatus={buttonStatus}
              email={email}
              emailValidation={this.formFieldTypeFactory(emailError)}
              password={password}
              passwordValidation={this.formFieldTypeFactory(passwordError)}
              handleInputChange={this.handleInputChange}
              handleForgotPasswordClick={this.handleForgotPasswordClick}
            />
          </Panel.Body>
        </Panel>
      </form>
    )
  }

  private get validateFieldValues(): {
    isValid: boolean
    errors: ErrorObject
  } {
    const {email, password} = this.state

    const emailError = email === '' ? 'Email is required' : ''
    const passwordError = password === '' ? 'Password is required' : ''

    const errors: ErrorObject = {
      emailError,
      passwordError,
    }

    const isValid = Object.values(errors).every(error => error === '')

    return {isValid, errors}
  }

  private formFieldTypeFactory = (
    errorMessage: string
  ): FormFieldValidation => ({
    errorMessage,
    hasError: errorMessage !== '',
  })

  private handleSubmit = (event: FormEvent) => {
    const {isValid, errors} = this.validateFieldValues
    const {email, password} = this.state

    event.preventDefault()

    if (!isValid) {
      this.setState(errors)
      return
    }

    this.setState({buttonStatus: ComponentStatus.Loading})

    this.auth0.login(
      {
        realm: Auth0Connection.Authentication,
        email,
        password,
      },
      error => {
        if (error) {
          this.setState({buttonStatus: ComponentStatus.Default})
          return this.displayErrorMessage(errors, error)
        }
      }
    )
    return
  }

  private displayErrorMessage = (errors, auth0Err) => {
    // eslint-disable-next-line
    if (/error in email/.test(auth0Err.code)) {
      this.setState({
        ...errors,
        emailError: 'Please enter a valid email address',
      })
    } else if (auth0Err.code === 'access_denied') {
      const emailError = `The email and password combination you submitted don't match. Please try again`
      this.setState({...errors, emailError})
    } else {
      const emailError = `We have been notified of an issue while accessing your account. If this issue persists, please contact support@influxdata.com`
      this.setState({...errors, emailError})
    }
  }

  private handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    this.setState({[event.target.name]: event.target.value})
  }

  private handleSocialClick = (connection: Auth0Connection) => {
    this.auth0.authorize({
      connection,
    })
  }

  private handleForgotPasswordClick = event => {
    event.preventDefault()
    const {email} = this.state
    const {onNotify} = this.props
    if (!email) {
      this.setState({emailError: 'Please enter a valid email address'})
      return
    }
    this.auth0.changePassword(
      {
        email,
        connection: Auth0Connection.Authentication,
      },
      (error, successMessage) => {
        if (error) {
          this.setState({emailError: error.message})
          return
        }
        // notify user that change password email was sent successfully
        // By default auth0 will send a success message even if the operation fails:
        // https://community.auth0.com/t/auth0-changepassword-always-returns-ok-even-when-user-is-not-found/11081/8
        onNotify(passwordResetSuccessfully(successMessage))
      }
    )
  }
}

const mdtp: DispatchProps = {
  onNotify: notify,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(LoginPageContents)

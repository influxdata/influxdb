// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import {connect} from 'react-redux'
import {
  AlignItems,
  ComponentColor,
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
  SelectGroup,
  Typeface,
} from '@influxdata/clockface'
import auth0js, {WebAuth} from 'auth0-js'

// Components
import {LoginForm} from 'src/onboarding/components/LoginForm'
import {SignUpForm} from 'src/onboarding/components/SignUpForm'
import {SocialButton} from 'src/shared/components/SocialButton'
import {GoogleLogo, GithubLogo} from 'src/clientLibraries/graphics'
import {Transition, animated} from 'react-spring/renderprops'

// Types
import {Auth0Connection, FormFieldValidation} from 'src/types'

// APIs & Actions
import {notify} from 'src/shared/actions/notifications'
import {passwordResetSuccessfully} from 'src/shared/copy/notifications'
import {getAuth0Config} from 'src/authorizations/apis'

interface ErrorObject {
  [key: string]: string | undefined
}

interface DispatchProps {
  onNotify: typeof notify
}

enum ActiveTab {
  SignUp = 'signup',
  Login = 'login',
}

interface State {
  activeTab: ActiveTab
  buttonStatus: ComponentStatus
  confirmPassword: string
  confirmPasswordError: string
  email: string
  emailError: string
  firstName: string
  firstNameError: string
  lastName: string
  lastNameError: string
  password: string
  passwordError: string
}

class LoginPageContents extends PureComponent<DispatchProps> {
  private auth0: typeof WebAuth

  state: State = {
    activeTab: ActiveTab.Login,
    buttonStatus: ComponentStatus.Default,
    confirmPassword: '',
    confirmPasswordError: '',
    email: '',
    emailError: '',
    firstName: '',
    firstNameError: '',
    lastName: '',
    lastNameError: '',
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
      activeTab,
      buttonStatus,
      confirmPassword,
      confirmPasswordError,
      email,
      emailError,
      firstName,
      firstNameError,
      lastName,
      lastNameError,
      password,
      passwordError,
    } = this.state

    const loginTabActive = activeTab === ActiveTab.Login

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
                  <SocialButton
                    buttonText="Github"
                    handleClick={() => {
                      this.handleSocialClick(Auth0Connection.Github)
                    }}
                  >
                    <GithubLogo className="signup-icon" />
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
            <FlexBox
              stretchToFitWidth={true}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.Center}
            >
              <SelectGroup
                size={ComponentSize.Large}
                color={ComponentColor.Default}
              >
                <SelectGroup.Option
                  titleText="Log In"
                  value={ActiveTab.Login}
                  id="login-option"
                  active={loginTabActive}
                  onClick={this.handleTabChange}
                >
                  Log In
                </SelectGroup.Option>
                <SelectGroup.Option
                  titleText="Sign Up"
                  value={ActiveTab.SignUp}
                  id="signup-option"
                  active={!loginTabActive}
                  onClick={this.handleTabChange}
                >
                  Sign Up
                </SelectGroup.Option>
              </SelectGroup>
            </FlexBox>
            <Transition
              native
              reset
              unique
              config={{
                mass: 0.25,
                precision: 1,
                friction: 1,
                clamp: true,
              }}
              items={loginTabActive}
              from={{height: 0}}
              enter={[
                {
                  position: 'relative',
                  overflow: 'hidden',
                  height: 'auto',
                },
              ]}
              leave={{height: 0}}
            >
              {shouldShow =>
                shouldShow &&
                (props => (
                  <animated.div style={props}>
                    <LoginForm
                      buttonStatus={buttonStatus}
                      email={email}
                      emailValidation={this.formFieldTypeFactory(emailError)}
                      password={password}
                      passwordValidation={this.formFieldTypeFactory(
                        passwordError
                      )}
                      handleInputChange={this.handleInputChange}
                      handleForgotPasswordClick={this.handleForgotPasswordClick}
                    />
                  </animated.div>
                ))
              }
            </Transition>
            <Transition
              native
              reset
              unique
              config={{
                mass: 0.25,
                precision: 1,
                friction: 1,
                clamp: true,
              }}
              items={loginTabActive === false}
              from={{height: 0}}
              enter={[
                {
                  position: 'relative',
                  overflow: 'hidden',
                  height: 'auto',
                },
              ]}
              leave={{height: 0}}
            >
              {shouldShow =>
                shouldShow &&
                (props => (
                  <animated.div style={props}>
                    <SignUpForm
                      buttonStatus={buttonStatus}
                      confirmPassword={confirmPassword}
                      confirmPasswordValidation={this.formFieldTypeFactory(
                        confirmPasswordError
                      )}
                      email={email}
                      emailValidation={this.formFieldTypeFactory(emailError)}
                      firstName={firstName}
                      firstNameValidation={this.formFieldTypeFactory(
                        firstNameError
                      )}
                      lastName={lastName}
                      lastNameValidation={this.formFieldTypeFactory(
                        lastNameError
                      )}
                      password={password}
                      passwordValidation={this.formFieldTypeFactory(
                        passwordError
                      )}
                      handleInputChange={this.handleInputChange}
                    />
                  </animated.div>
                ))
              }
            </Transition>
          </Panel.Body>
        </Panel>
      </form>
    )
  }

  private get validateFieldValues(): {
    isValid: boolean
    errors: {[fieldName: string]: string}
  } {
    const {
      activeTab,
      firstName,
      lastName,
      email,
      password,
      confirmPassword,
    } = this.state

    const passwordsMatch = confirmPassword === password

    const firstNameError = firstName === '' ? 'First name is required' : ''
    const lastNameError = lastName === '' ? 'Last name is required' : ''
    const emailError = email === '' ? 'Email is required' : ''
    const passwordError = password === '' ? 'Password is required' : ''

    let confirmPasswordError = passwordsMatch
      ? ''
      : "The input passwords don't match"
    if (confirmPassword === '') {
      confirmPasswordError = 'Confirm password is required'
    }

    const errors: ErrorObject = {
      emailError,
      passwordError,
    }
    if (activeTab === ActiveTab.SignUp) {
      errors.firstNameError = firstNameError
      errors.lastNameError = lastNameError
      errors.confirmPasswordError = confirmPasswordError
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
    const {
      email,
      password,
      firstName: given_name,
      lastName: family_name,
      activeTab,
    } = this.state

    event.preventDefault()

    if (!isValid) {
      this.setState(errors)
      return
    }

    this.setState({buttonStatus: ComponentStatus.Loading})

    if (activeTab === ActiveTab.Login) {
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

    this.auth0.signup(
      {
        connection: Auth0Connection.Authentication,
        email,
        password,
        family_name,
        given_name,
      },
      error => {
        if (error) {
          this.displayErrorMessage(errors, error)
          this.setState({buttonStatus: ComponentStatus.Default})
          return
        }
        // log the user into Auth0
        this.auth0.login(
          {
            realm: Auth0Connection.Authentication,
            email,
            password,
          },
          error => {
            if (error) {
              this.setState({buttonStatus: ComponentStatus.Default})
              this.displayErrorMessage(errors, error)
              return
            }
          }
        )
      }
    )
  }

  private displayErrorMessage = (errors, auth0Err) => {
    const {activeTab} = this.state
    // eslint-disable-next-line
    if (/error in email/.test(auth0Err.code)) {
      this.setState({
        ...errors,
        emailError: 'Please enter a valid email address',
      })
    } else if (
      auth0Err.code === 'access_denied' ||
      auth0Err.code === 'user_exists'
    ) {
      if (activeTab === ActiveTab.Login) {
        const emailError = `The email and password combination you submitted are don't match. Please try again`
        this.setState({...errors, emailError})
      } else {
        const emailError = `An account with that email address already exists.  Try logging in instead.`
        this.setState({...errors, emailError})
      }
    } else {
      const emailError = `We have been notified of an issue while accessing your account. If this issue persists, please contact support@influxdata.com`
      this.setState({...errors, emailError})
    }
  }

  private handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    this.setState({[event.target.name]: event.target.value})
  }

  private handleTabChange = (value: ActiveTab) => {
    this.setState({
      activeTab: value,
      confirmPassword: '',
      confirmPasswordError: '',
      email: '',
      emailError: '',
      firstName: '',
      firstNameError: '',
      lastName: '',
      lastNameError: '',
      password: '',
      passwordError: '',
    })
  }

  private handleSocialClick = (connection: Auth0Connection) => {
    this.auth0.authorize({
      connection,
    })
  }

  private handleForgotPasswordClick = () => {
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

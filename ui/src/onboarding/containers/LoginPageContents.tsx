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
  Grid,
  JustifyContent,
  Method,
  Panel,
  SelectGroup,
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

interface State {
  activeTab: string
  buttonStatus: ComponentStatus
  firstName: string
  lastName: string
  email: string
  password: string
  confirmPassword: string
  firstNameError: string
  lastNameError: string
  emailError: string
  passwordError: string
  confirmPasswordError: string
}

class LoginPageContents extends PureComponent<DispatchProps> {
  private auth0?: typeof WebAuth

  state: State = {
    activeTab: 'login',
    buttonStatus: ComponentStatus.Default,
    firstName: '',
    lastName: '',
    email: '',
    password: '',
    confirmPassword: '',
    firstNameError: '',
    lastNameError: '',
    emailError: '',
    passwordError: '',
    confirmPasswordError: '',
  }

  public async componentDidMount() {
    const config = await getAuth0Config()
    this.auth0 = auth0js.WebAuth({
      domain: config.domain,
      clientID: config.clientID,
      redirectUri: config.redirectURL,
      responseType: 'code',
    })
  }

  render() {
    const {
      buttonStatus,
      firstName,
      lastName,
      email,
      password,
      confirmPassword,
      firstNameError,
      lastNameError,
      emailError,
      passwordError,
      confirmPasswordError,
      activeTab,
    } = this.state

    const loginTabActive = activeTab === 'login'

    return (
      <form
        action="/signup"
        method={Method.Post}
        onSubmit={this.handleSubmit}
        className="sign-up--form"
      >
        <div className="sign-up--login-container">
          <h2>Create your Free InfluxDB Cloud Account</h2>
          <p className="sign-up--login-text">No credit card required</p>
        </div>
        <Panel className="sign-up--form-panel">
          <Panel.Header size={ComponentSize.Large}>
            <Grid>
              <Grid.Row>
                <p className="sign-up--social-header">Continue with</p>
              </Grid.Row>
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
          </Panel.Header>
          <div className="sign-up--or">
            <p className="sign-up--social-header">OR</p>
          </div>
          <Panel.Body size={ComponentSize.Large}>
            <div>
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
                    titleText="Login"
                    value="login"
                    id="login-option"
                    active={loginTabActive}
                    onClick={this.handleTabChange}
                  >
                    Login
                  </SelectGroup.Option>
                  <SelectGroup.Option
                    titleText="Sign Up"
                    value="signup"
                    id="signup-option"
                    active={!loginTabActive}
                    onClick={this.handleTabChange}
                  >
                    Sign Up
                  </SelectGroup.Option>
                </SelectGroup>
              </FlexBox>
            </div>
            <Transition
              native
              reset
              unique
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
              {show =>
                show &&
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
              {show =>
                show &&
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

    const firstNameError = firstName === '' && 'First name is required'
    const lastNameError = lastName === '' && 'Last name is required'
    const emailError = email === '' && 'Email is required'
    const passwordError = password === '' && 'Password is required'
    const confirmPasswordError =
      confirmPassword === password
        ? confirmPassword === '' && 'Confirm password is required'
        : "The input passwords don't match"

    const errors: ErrorObject = {
      emailError,
      passwordError,
    }
    if (activeTab === 'signup') {
      errors.firstNameError = firstNameError
      errors.lastNameError = lastNameError
      errors.confirmPasswordError = confirmPasswordError
    }

    const isValid = !Object.values(errors).some(error => !!error)

    return {isValid, errors}
  }

  private formFieldTypeFactory = (
    errorMessage: string
  ): FormFieldValidation => ({
    errorMessage,
    isValid: errorMessage !== '',
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

    if (activeTab === 'login') {
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
        // log the user into Quartz
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
    if (auth0Err.code.includes('error in email')) {
      this.setState({
        ...errors,
        emailError: 'Please enter a valid email address',
      })
    } else if (auth0Err.code === 'user_exists') {
      const emailError = `An account with that email address already exists. Try logging in instead.`
      this.setState({...errors, emailError})
    } else {
      const emailError = `We have been notified of an issue while creating your account. If this issue persists, please contact support@influxdata.com`
      this.setState({...errors, emailError})
    }
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({[e.target.name]: e.target.value})
  }

  private handleTabChange = (value: string) => {
    this.setState({activeTab: value})
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

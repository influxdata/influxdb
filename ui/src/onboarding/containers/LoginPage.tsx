// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {
  AlignItems,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  FlexBox,
  FlexDirection,
  Gradients,
  Grid,
  JustifyContent,
  Method,
  Notification,
  Panel,
  SelectGroup,
} from '@influxdata/clockface'
import auth0js from 'auth0-js'
import {get} from 'lodash'

// Components
// import CSRF from 'js/components/CSRF' // TODO: figure out if obtaining the CSRF token is necessary or if it even exists
import {Transition, animated} from 'react-spring/renderprops'
import LoginForm from 'src/onboarding/components/LoginForm'
import SignUpForm from 'src/onboarding/components/SignUpForm'
import SocialButton from 'src/shared/components/SocialButton'
import {GoogleLogo, GithubLogo} from 'src/clientLibraries/graphics'

// Types
import {Auth0Connection} from 'src/types'

// APIs
import {postSignin} from 'src/client'

// Constants
import {CLOUD_URL} from 'src/shared/constants'

const auth0 = new auth0js.WebAuth({
  domain: 'influxdata-dev.auth0.com', // TODO: get from IDPE or somewhere else
  clientID: 'bnqXbv51ISpm9Z8vl0wVZEFYEJTVzjoE', // TODO: get from IDPE or somewhere else
  redirectUri: 'http://localhost:4000/auth0/callback/signup', // TODO: get from IDPE or somewhere else
  responseType: 'code',
})

interface ErrorObject {
  [key: string]: any
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
  notificationText: string
  notificationVisible: boolean
}

class LoginPage extends PureComponent<WithRouterProps> {
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
    notificationText: '',
    notificationVisible: false,
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
      notificationText,
      notificationVisible,
    } = this.state

    const loginTabActive = activeTab === 'login'
    const signupTabActive = activeTab === 'signup'

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
                    active={signupTabActive}
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
                      emailError={emailError}
                      password={password}
                      passwordError={passwordError}
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
              items={signupTabActive}
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
                      confirmPasswordError={confirmPasswordError}
                      email={email}
                      emailError={emailError}
                      firstName={firstName}
                      firstNameError={firstNameError}
                      lastName={lastName}
                      lastNameError={lastNameError}
                      password={password}
                      passwordError={passwordError}
                      handleInputChange={this.handleInputChange}
                    />
                  </animated.div>
                ))
              }
            </Transition>
          </Panel.Body>
          <Notification
            visible={notificationVisible}
            size={ComponentSize.Medium}
            gradient={Gradients.GarageBand}
          >
            {notificationText}
          </Notification>
          {/* <CSRF /> */}
        </Panel>
      </form>
    )
  }

  private get validFieldValues(): {
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

    const firstNameError = !firstName && 'This field is required'
    const lastNameError = !lastName && 'This field is required'
    const emailError = !email && 'This field is required'
    const passwordError = !password && 'This field is required'
    const confirmPasswordError =
      confirmPassword === password
        ? !confirmPassword && 'This field is required'
        : 'Passwords must match'

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

  private handleRedirect() {
    const {router} = this.props
    const {query} = this.props.location

    console.log('query: ', query)

    if (query && query.returnTo) {
      router.replace(query.returnTo)
    } else {
      router.push('/')
    }
  }

  private handleLogin = async () => {
    const {email, password} = this.state
    const {errors} = this.validFieldValues
    try {
      const resp = await postSignin({auth: {username: email, password}})
      if (resp.status !== 204) {
        throw new Error(resp.data.message)
      }

      this.handleRedirect()
    } catch (error) {
      if (error) {
        this.setState({buttonStatus: ComponentStatus.Default})
        return this.displayErrorMessage(errors, error)
      }
    }
  }

  private handleSubmit = (e: FormEvent) => {
    const {isValid, errors} = this.validFieldValues
    const {
      email,
      password,
      firstName: given_name,
      lastName: family_name,
      activeTab,
    } = this.state

    e.preventDefault()

    if (!isValid) {
      this.setState(errors)
      return
    }

    this.setState({buttonStatus: ComponentStatus.Loading})

    if (activeTab === 'login') {
      // login with credentials to Quartz
      // TOOD: figure out if we want to log users to auth0 in addition to IDPE
      // auth0.login(
      //   {
      //     realm: Auth0Connection.Authentication,
      //     email,
      //     password,
      //   },
      //   err => {
      //     if (err) {
      //       this.setState({buttonStatus: ComponentStatus.Default})
      //       return this.displayErrorMessage(errors, err)
      //     }
      // logs user in IDPE
      this.handleLogin()
      //   }
      // )
      return
    }

    auth0.signup(
      {
        connection: Auth0Connection.Authentication,
        email,
        password,
        family_name,
        given_name,
      },
      err => {
        if (err) {
          this.displayErrorMessage(errors, err)
          this.setState({buttonStatus: ComponentStatus.Default})
          return
        }
        // log the user into Quartz
        auth0.login(
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
            // logs user in IDPE
            this.handleLogin()
          }
        )
      }
    )
  }

  private displayErrorMessage = (errors, auth0Err) => {
    if (/error in email/.test(auth0Err.code)) {
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
      throw new Error(auth0Err.description)
    }
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({[e.target.name]: e.target.value})
  }

  private handleTabChange = (value: string) => {
    this.setState({activeTab: value})
  }

  private handleSocialClick = (connection: Auth0Connection) => {
    auth0.authorize({
      connection,
      redirectUri: 'http://localhost:4000/auth0/callback/login',
    })
  }

  private handleForgotPasswordClick = () => {
    const {email} = this.state
    if (!email) {
      this.setState({emailError: 'Please enter a valid email address'})
      return
    }
    auth0.changePassword(
      {
        email,
        connection: Auth0Connection.Authentication,
      },
      (err, successMessage) => {
        if (err) {
          this.setState({emailError: err.message})
          return
        }
        // notify user that change password email was sent successfully
        // By default auth0 will send a success message even if the operation fails:
        // https://community.auth0.com/t/auth0-changepassword-always-returns-ok-even-when-user-is-not-found/11081/8
        this.setState({
          emailError: '',
          notificationText: `${successMessage}
            If you haven't received an email, please ensure that the email you provided is correct.`,
          notificationVisible: true,
        })
        // sets a time to remove the notification
        setTimeout(() => {
          this.setState({
            notificationText: '',
            notificationVisible: false,
          })
        }, 4000)
      }
    )
  }
}

export default withRouter(LoginPage)

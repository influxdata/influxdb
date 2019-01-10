// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {getDeep} from 'src/utils/wrappers'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  ComponentSize,
  Input,
  InputType,
  Form,
  Columns,
  IconFont,
  Grid,
  ComponentStatus,
} from 'src/clockface'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// APIS
import {setSetupParams, SetupParams, signin} from 'src/onboarding/apis'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {StepStatus} from 'src/clockface/constants/wizard'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface State extends SetupParams {
  confirmPassword: string
  isAlreadySet: boolean
  isPassMismatched: boolean
}

@ErrorHandling
class AdminStep extends PureComponent<OnboardingStepProps, State> {
  constructor(props: OnboardingStepProps) {
    super(props)
    const {setupParams} = props
    this.state = {
      username: getDeep(setupParams, 'username', ''),
      password: getDeep(setupParams, 'password', ''),
      confirmPassword: getDeep(setupParams, 'password', ''),
      org: getDeep(setupParams, 'org', ''),
      bucket: getDeep(setupParams, 'bucket', ''),
      isAlreadySet: !!setupParams,
      isPassMismatched: false,
    }
  }

  public render() {
    const {username, password, confirmPassword, org, bucket} = this.state
    const icon = this.InputIcon
    const status = this.InputStatus
    return (
      <div className="onboarding-step">
        <Form onSubmit={this.handleNext}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <h3 className="wizard-step--title">Setup Initial User</h3>
                <h5 className="wizard-step--sub-title">
                  You will be able to create additional Users, Buckets and
                  Organizations later
                </h5>
                <Grid>
                  <Grid.Row>
                    <Grid.Column
                      widthXS={Columns.Twelve}
                      widthMD={Columns.Ten}
                      offsetMD={Columns.One}
                    >
                      <Form.Element label="Username">
                        <Input
                          value={username}
                          onChange={this.handleUsername}
                          titleText="Username"
                          size={ComponentSize.Medium}
                          icon={icon}
                          status={status}
                          disabledTitleText="Username has been set"
                          autoFocus={true}
                        />
                      </Form.Element>
                    </Grid.Column>
                    <Grid.Column
                      widthXS={Columns.Six}
                      widthMD={Columns.Five}
                      offsetMD={Columns.One}
                    >
                      <Form.Element label="Password">
                        <Input
                          type={InputType.Password}
                          value={password}
                          onChange={this.handlePassword}
                          titleText="Password"
                          size={ComponentSize.Medium}
                          icon={icon}
                          status={status}
                          disabledTitleText="Password has been set"
                        />
                      </Form.Element>
                    </Grid.Column>
                    <Grid.Column widthXS={Columns.Six} widthMD={Columns.Five}>
                      <Form.Element label="Confirm Password">
                        <Input
                          type={InputType.Password}
                          value={confirmPassword}
                          onChange={this.handleConfirmPassword}
                          titleText="Confirm Password"
                          size={ComponentSize.Medium}
                          icon={icon}
                          status={this.passwordStatus}
                          disabledTitleText="password has been set"
                        />
                      </Form.Element>
                    </Grid.Column>
                    <Grid.Column
                      widthXS={Columns.Twelve}
                      widthMD={Columns.Ten}
                      offsetMD={Columns.One}
                    >
                      <Form.Element label="Default Organization Name">
                        <Input
                          value={org}
                          onChange={this.handleOrg}
                          titleText="Default Organization Name"
                          size={ComponentSize.Medium}
                          icon={icon}
                          status={ComponentStatus.Default}
                          placeholder="Your organization is where everything you create lives"
                          disabledTitleText="Default organization name has been set"
                        />
                      </Form.Element>
                    </Grid.Column>
                    <Grid.Column
                      widthXS={Columns.Twelve}
                      widthMD={Columns.Ten}
                      offsetMD={Columns.One}
                    >
                      <Form.Element label="Default Bucket Name">
                        <Input
                          value={bucket}
                          onChange={this.handleBucket}
                          titleText="Default Bucket Name"
                          size={ComponentSize.Medium}
                          icon={icon}
                          status={status}
                          placeholder="Your bucket is where you will store all your data"
                          disabledTitleText="Default bucket name has been set"
                        />
                      </Form.Element>
                    </Grid.Column>
                  </Grid.Row>
                </Grid>
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={this.props.onDecrementCurrentStepIndex}
            nextButtonText={this.nextButtonText}
            backButtonText="Back to Start"
            nextButtonStatus={this.nextButtonStatus}
            autoFocusNext={false}
          />
        </Form>
      </div>
    )
  }

  private handleUsername = (e: ChangeEvent<HTMLInputElement>): void => {
    const username = e.target.value
    this.setState({username})
  }

  private handlePassword = (e: ChangeEvent<HTMLInputElement>): void => {
    const {confirmPassword} = this.state
    const password = e.target.value
    const isPassMismatched = confirmPassword && password !== confirmPassword
    this.setState({password, isPassMismatched})
  }

  private handleConfirmPassword = (e: ChangeEvent<HTMLInputElement>): void => {
    const {password} = this.state
    const confirmPassword = e.target.value
    const isPassMismatched = confirmPassword && password !== confirmPassword
    this.setState({confirmPassword, isPassMismatched})
  }

  private handleOrg = (e: ChangeEvent<HTMLInputElement>): void => {
    const org = e.target.value
    this.setState({org})
  }

  private handleBucket = (e: ChangeEvent<HTMLInputElement>): void => {
    const bucket = e.target.value
    this.setState({bucket})
  }

  private get nextButtonStatus(): ComponentStatus {
    if (this.areInputsValid) {
      return ComponentStatus.Default
    }
    return ComponentStatus.Disabled
  }

  private get areInputsValid(): boolean {
    const {
      username,
      password,
      org,
      bucket,
      confirmPassword,
      isPassMismatched,
    } = this.state

    return (
      username &&
      password &&
      confirmPassword &&
      org &&
      bucket &&
      !isPassMismatched
    )
  }

  private get nextButtonText(): string {
    if (this.areInputsValid) {
    }
    return 'Continue to Data Source Selection'
    return 'All fields are required to continue'
  }

  private get passwordStatus(): ComponentStatus {
    const {isAlreadySet, isPassMismatched} = this.state
    if (isAlreadySet) {
      return ComponentStatus.Disabled
    }
    if (isPassMismatched) {
      return ComponentStatus.Error
    }
    return ComponentStatus.Default
  }

  private get InputStatus(): ComponentStatus {
    const {isAlreadySet} = this.state
    if (isAlreadySet) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }

  private get InputIcon(): IconFont {
    const {isAlreadySet} = this.state
    if (isAlreadySet) {
      return IconFont.Checkmark
    }
    return null
  }

  private handleNext = async () => {
    const {
      onSetStepStatus,
      currentStepIndex,
      handleSetSetupParams,
      notify,
      onIncrementCurrentStepIndex,
    } = this.props

    const {username, password, org, bucket, isAlreadySet} = this.state

    if (isAlreadySet) {
      onSetStepStatus(currentStepIndex, StepStatus.Complete)
      onIncrementCurrentStepIndex()
      return
    }

    const setupParams = {
      username,
      password,
      org,
      bucket,
    }

    try {
      await setSetupParams(setupParams)
      await signin({username, password})
      notify(copy.SetupSuccess)
      handleSetSetupParams(setupParams)
      onSetStepStatus(currentStepIndex, StepStatus.Complete)
      onIncrementCurrentStepIndex()
    } catch (error) {
      notify(copy.SetupError)
    }
  }
}

export default AdminStep

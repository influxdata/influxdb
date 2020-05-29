// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {getDeep} from 'src/utils/wrappers'

// Components
import {Form, Input, Grid, QuestionMarkTooltip} from '@influxdata/clockface'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Actions
import {setupAdmin} from 'src/onboarding/actions'

// Types
import {ISetupParams} from '@influxdata/influx'
import {
  Columns,
  IconFont,
  InputType,
  ComponentSize,
  ComponentStatus,
  ComponentColor,
} from '@influxdata/clockface'
import {StepStatus} from 'src/clockface/constants/wizard'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface State extends ISetupParams {
  confirmPassword: string
  isPassMismatched: boolean
  isPassTooShort: boolean
}

interface Props extends OnboardingStepProps {
  onSetupAdmin: typeof setupAdmin
}

@ErrorHandling
class AdminStep extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)
    const {setupParams} = props

    const username = getDeep(setupParams, 'username', '')
    const password = getDeep(setupParams, 'password', '')
    const confirmPassword = getDeep(setupParams, 'password', '')
    const org = getDeep(setupParams, 'org', '')
    const bucket = getDeep(setupParams, 'bucket', '')

    this.state = {
      username,
      password,
      confirmPassword,
      org,
      bucket,
      isPassMismatched: false,
      isPassTooShort: false,
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
            <div className="wizard-step--scroll-content">
              <h3
                className="wizard-step--title"
                data-testid="admin-step--head-main"
              >
                Setup Initial User
              </h3>
              <h5
                className="wizard-step--sub-title"
                data-testid="admin-step--head-sub"
              >
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
                        testID="input-field--username"
                      />
                    </Form.Element>
                  </Grid.Column>
                  <Grid.Column
                    widthXS={Columns.Six}
                    widthMD={Columns.Five}
                    offsetMD={Columns.One}
                  >
                    <Form.Element
                      label="Password"
                      errorMessage={this.passwordError}
                    >
                      <Input
                        type={InputType.Password}
                        value={password}
                        onChange={this.handlePassword}
                        titleText="Password"
                        size={ComponentSize.Medium}
                        icon={icon}
                        status={status}
                        disabledTitleText="Password has been set"
                        testID="input-field--password"
                      />
                    </Form.Element>
                  </Grid.Column>
                  <Grid.Column widthXS={Columns.Six} widthMD={Columns.Five}>
                    <Form.Element
                      label="Confirm Password"
                      errorMessage={this.confirmError}
                    >
                      <Input
                        type={InputType.Password}
                        value={confirmPassword}
                        onChange={this.handleConfirmPassword}
                        titleText="Confirm Password"
                        size={ComponentSize.Medium}
                        icon={icon}
                        status={this.passwordStatus}
                        disabledTitleText="password has been set"
                        testID="input-field--password-chk"
                      />
                    </Form.Element>
                  </Grid.Column>
                  <Grid.Column
                    widthXS={Columns.Twelve}
                    widthMD={Columns.Ten}
                    offsetMD={Columns.One}
                  >
                    <Form.Element
                      label="Initial Organization Name"
                      labelAddOn={this.orgTip}
                      testID="form-elem--orgname"
                    >
                      <Input
                        value={org}
                        onChange={this.handleOrg}
                        titleText="Initial Organization Name"
                        size={ComponentSize.Medium}
                        icon={icon}
                        status={ComponentStatus.Default}
                        placeholder="An organization is a workspace for a group of users."
                        disabledTitleText="Initial organization name has been set"
                        testID="input-field--orgname"
                      />
                    </Form.Element>
                  </Grid.Column>
                  <Grid.Column
                    widthXS={Columns.Twelve}
                    widthMD={Columns.Ten}
                    offsetMD={Columns.One}
                  >
                    <Form.Element
                      label="Initial Bucket Name"
                      labelAddOn={this.bucketTip}
                      testID="form-elem--bucketname"
                    >
                      <Input
                        value={bucket}
                        onChange={this.handleBucket}
                        titleText="Initial Bucket Name"
                        size={ComponentSize.Medium}
                        icon={icon}
                        status={status}
                        placeholder="A bucket is where your time series data is stored with a retention policy."
                        disabledTitleText="Initial bucket name has been set"
                        testID="input-field--bucketname"
                      />
                    </Form.Element>
                  </Grid.Column>
                </Grid.Row>
              </Grid>
            </div>
          </div>
          <OnboardingButtons
            nextButtonStatus={this.nextButtonStatus}
            autoFocusNext={false}
          />
        </Form>
      </div>
    )
  }

  private get passwordError(): string {
    return this.state.isPassTooShort && 'Password must be at least 8 characters'
  }

  private get confirmError(): string {
    return this.state.isPassMismatched && 'Passwords do not match'
  }

  private get isAdminSet(): boolean {
    const {stepStatuses, currentStepIndex} = this.props
    if (stepStatuses[currentStepIndex] === StepStatus.Complete) {
      return true
    }
    return false
  }

  private handleUsername = (e: ChangeEvent<HTMLInputElement>): void => {
    const username = e.target.value
    this.setState({username})
  }

  private handlePassword = (e: ChangeEvent<HTMLInputElement>): void => {
    const {confirmPassword} = this.state
    const password = e.target.value
    const isPassMismatched = confirmPassword && password !== confirmPassword
    const isPassTooShort = password && password.length < 8
    this.setState({password, isPassMismatched, isPassTooShort})
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

  private orgTip = (): JSX.Element => {
    return (
      <QuestionMarkTooltip
        diameter={16}
        style={{marginLeft: '8px'}}
        color={ComponentColor.Primary}
        testID="admin_org_tooltip"
        tooltipStyle={{width: '300px'}}
        tooltipContents="An organization is a workspace for a group of users requiring access to time series data, dashboards, and other resources.
        You can create organizations for different functional groups, teams, or projects."
      />
    )
  }

  private bucketTip = (): JSX.Element => {
    return (
      <QuestionMarkTooltip
        diameter={16}
        style={{marginLeft: '8px'}}
        color={ComponentColor.Primary}
        testID="admin_bucket_tooltip"
        tooltipStyle={{width: '300px'}}
        tooltipContents="A bucket is where your time series data is stored with a retention policy."
      />
    )
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
      isPassTooShort,
    } = this.state

    return (
      username &&
      password &&
      confirmPassword &&
      org &&
      bucket &&
      !isPassMismatched &&
      !isPassTooShort
    )
  }

  private get passwordStatus(): ComponentStatus {
    const {isPassMismatched} = this.state
    if (this.isAdminSet) {
      return ComponentStatus.Disabled
    }
    if (isPassMismatched) {
      return ComponentStatus.Error
    }
    return ComponentStatus.Default
  }

  private get InputStatus(): ComponentStatus {
    if (this.isAdminSet) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }

  private get InputIcon(): IconFont {
    if (this.isAdminSet) {
      return IconFont.Checkmark
    }
    return null
  }

  private handleNext = async () => {
    const {
      onIncrementCurrentStepIndex,
      onSetupAdmin: onSetupAdmin,
      onSetStepStatus,
      currentStepIndex,
    } = this.props

    const {username, password, org, bucket} = this.state

    if (this.isAdminSet) {
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

    const isAdminSet = await onSetupAdmin(setupParams)
    if (isAdminSet) {
      onIncrementCurrentStepIndex()
    }
  }
}

export default AdminStep

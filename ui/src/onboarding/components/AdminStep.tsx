// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {getDeep} from 'src/utils/wrappers'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  Input,
  InputType,
  Form,
  Columns,
  IconFont,
  ComponentStatus,
} from 'src/clockface'

// APIS
import {setSetupParams, SetupParams, signin} from 'src/onboarding/apis'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {StepStatus} from 'src/clockface/constants/wizard'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface State extends SetupParams {
  isAlreadySet: boolean
}

@ErrorHandling
class AdminStep extends PureComponent<OnboardingStepProps, State> {
  constructor(props) {
    super(props)
    const {setupParams} = props
    this.state = {
      username: getDeep(setupParams, 'username', ''),
      password: getDeep(setupParams, 'password', ''),
      org: getDeep(setupParams, 'org', ''),
      bucket: getDeep(setupParams, 'bucket', ''),
      isAlreadySet: !!setupParams,
    }
  }
  public render() {
    const {username, password, org, bucket, isAlreadySet} = this.state
    let icon = null
    let status = ComponentStatus.Default
    if (isAlreadySet) {
      icon = IconFont.Checkmark
      status = ComponentStatus.Disabled
    }
    return (
      <div className="onboarding-step">
        <h3 className="wizard-step--title">Setup Admin User</h3>
        <h5 className="wizard-step--sub-title">
          You will be able to create additional Users, Buckets and Organizations
          later.
        </h5>
        <Form className="onboarding--admin-user-form">
          <Form.Element
            label="Admin Username"
            colsXS={Columns.Six}
            offsetXS={Columns.Three}
            errorMessage=""
          >
            <Input
              value={username}
              onChange={this.handleUsername}
              titleText="Admin Username"
              size={ComponentSize.Medium}
              icon={icon}
              status={status}
              disabledTitleText="Admin username has been set"
            />
          </Form.Element>
          <Form.Element
            label="Admin Password"
            colsXS={Columns.Six}
            offsetXS={Columns.Three}
          >
            <Input
              type={InputType.Password}
              value={password}
              onChange={this.handlePassword}
              titleText="Admin Password"
              size={ComponentSize.Medium}
              icon={icon}
              status={status}
              disabledTitleText="Admin password has been set"
            />
          </Form.Element>
          <Form.Element
            label="Default Organization Name"
            colsXS={Columns.Six}
            offsetXS={Columns.Three}
          >
            <Input
              value={org}
              onChange={this.handleOrg}
              titleText="Default Organization Name"
              size={ComponentSize.Medium}
              icon={icon}
              placeholder="Your organization is where everything you create lives"
              status={status}
              disabledTitleText="Default organization name has been set"
            />
          </Form.Element>
          <Form.Element
            label="Default Bucket Name"
            colsXS={Columns.Six}
            offsetXS={Columns.Three}
          >
            <Input
              value={bucket}
              onChange={this.handleBucket}
              titleText="Default Bucket Name"
              size={ComponentSize.Medium}
              icon={icon}
              placeholder="Your bucket is where you will store all your data"
              status={status}
              disabledTitleText="Default bucket name has been set"
            />
          </Form.Element>
        </Form>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={this.handlePrevious}
          />
          <Button
            color={ComponentColor.Primary}
            text="Next"
            size={ComponentSize.Medium}
            onClick={this.handleNext}
            status={this.nextButtonStatus}
            titleText={this.nextButtonTitle}
          />
        </div>
      </div>
    )
  }

  private handleUsername = (e: ChangeEvent<HTMLInputElement>): void => {
    const username = e.target.value
    this.setState({username})
  }
  private handlePassword = (e: ChangeEvent<HTMLInputElement>): void => {
    const password = e.target.value
    this.setState({password})
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
    const {username, password, org, bucket} = this.state
    if (username && password && org && bucket) {
      return ComponentStatus.Default
    }
    return ComponentStatus.Disabled
  }

  private get nextButtonTitle(): string {
    const {username, password, org, bucket} = this.state
    if (username && password && org && bucket) {
      return 'Next'
    }
    return 'All fields are required to continue'
  }

  private handleNext = async () => {
    const {
      links,
      handleSetStepStatus,
      currentStepIndex,
      handleSetSetupParams,
      notify,
    } = this.props

    const {username, password, org, bucket, isAlreadySet} = this.state

    if (isAlreadySet) {
      handleSetStepStatus(currentStepIndex, StepStatus.Complete)
      this.handleIncrement()
      return
    }

    const setupParams = {
      username,
      password,
      org,
      bucket,
    }

    try {
      await setSetupParams(links.setup, setupParams)
      await signin(links.signin, {username, password})
      notify(copy.SetupSuccess)
      handleSetSetupParams(setupParams)
      handleSetStepStatus(currentStepIndex, StepStatus.Complete)
      this.handleIncrement()
    } catch (error) {
      notify(copy.SetupError)
    }
  }

  private handlePrevious = () => {
    this.handleDecrement()
  }

  private handleIncrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex + 1)
  }

  private handleDecrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex - 1)
  }
}

export default AdminStep

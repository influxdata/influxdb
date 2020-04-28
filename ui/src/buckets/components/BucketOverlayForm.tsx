// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import moment from 'moment'

// Components
import {Form, Input, Button, Grid} from '@influxdata/clockface'
import Retention from 'src/buckets/components/Retention'

// Constants
import {MIN_RETENTION_SECONDS} from 'src/buckets/constants'
import {isSystemBucket} from 'src/buckets/constants/index'

// Types
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {RuleType} from 'src/buckets/reducers/createBucket'

interface Props {
  name: string
  retentionSeconds: number
  ruleType: 'expire'
  onSubmit: (e: FormEvent<HTMLFormElement>) => void
  onClose: () => void
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (t: RuleType) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  disableRenaming: boolean
  buttonText: string
  onClickRename?: () => void
}

export default class BucketOverlayForm extends PureComponent<Props> {
  public render() {
    const {
      name,
      onSubmit,
      ruleType,
      buttonText,
      retentionSeconds,
      disableRenaming,
      onClose,
      onChangeInput,
      onChangeRuleType,
      onChangeRetentionRule,
      onClickRename,
    } = this.props

    const nameInputStatus = disableRenaming && ComponentStatus.Disabled

    return (
      <Form onSubmit={onSubmit} testID="bucket-form">
        <Grid>
          <Grid.Row>
            <Grid.Column>
              <Form.ValidationElement
                value={name}
                label="Name"
                helpText={this.nameHelpText}
                validationFunc={this.handleNameValidation}
                required={true}
              >
                {status => (
                  <Input
                    status={nameInputStatus || status}
                    placeholder="Give your bucket a name"
                    name="name"
                    autoFocus={true}
                    value={name}
                    onChange={onChangeInput}
                    testID="bucket-form-name"
                  />
                )}
              </Form.ValidationElement>
              <Form.Element
                label="Delete Data"
                errorMessage={this.ruleErrorMessage}
              >
                <Retention
                  type={ruleType}
                  retentionSeconds={retentionSeconds}
                  onChangeRuleType={onChangeRuleType}
                  onChangeRetentionRule={onChangeRetentionRule}
                />
              </Form.Element>
            </Grid.Column>
          </Grid.Row>
          <Grid.Row>
            <Grid.Column>
              <Form.Footer>
                <Button
                  text="Cancel"
                  onClick={onClose}
                  type={ButtonType.Button}
                />
                {buttonText === 'Save Changes' && (
                  <Button
                    text="Rename"
                    color={ComponentColor.Danger}
                    onClick={onClickRename}
                  />
                )}
                <Button
                  text={buttonText}
                  testID="bucket-form-submit"
                  color={this.submitButtonColor}
                  status={this.submitButtonStatus}
                  type={ButtonType.Submit}
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private handleNameValidation = (name: string): string | null => {
    if (isSystemBucket(name)) {
      return 'Only system bucket names can begin with _'
    }

    if (!name) {
      return 'This bucket needs a name'
    }

    return null
  }

  private get nameHelpText(): string {
    if (this.props.disableRenaming) {
      return 'To rename bucket use the RENAME button below'
    }

    return ''
  }

  private get submitButtonColor(): ComponentColor {
    const {buttonText} = this.props

    if (buttonText === 'Save Changes') {
      return ComponentColor.Success
    }

    return ComponentColor.Primary
  }

  private get submitButtonStatus(): ComponentStatus {
    const {name} = this.props
    const nameHasErrors = this.handleNameValidation(name)

    if (nameHasErrors || this.retentionIsTooShort) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private get retentionIsTooShort(): boolean {
    const {retentionSeconds, ruleType} = this.props

    return ruleType === 'expire' && retentionSeconds < MIN_RETENTION_SECONDS
  }

  private get ruleErrorMessage(): string {
    if (this.retentionIsTooShort) {
      const humanDuration = moment
        .duration(MIN_RETENTION_SECONDS, 'seconds')
        .humanize()

      return `Retention period must be at least ${humanDuration}`
    }

    return ''
  }
}

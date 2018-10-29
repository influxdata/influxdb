import React, {PureComponent, ChangeEvent} from 'react'
// Components
import {
  Form,
  Input,
  Button,
  ComponentColor,
  ComponentStatus,
} from 'src/clockface'
import Retention from 'src/organizations/components/Retention'

// Types
import {RetentionRuleTypes} from 'src/types/v2'

interface Props {
  name: string
  errorMessage: string
  retentionSeconds: number
  ruleType: RetentionRuleTypes
  onSubmit: () => void
  onCloseModal: () => void
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (t: RetentionRuleTypes) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  nameInputStatus: ComponentStatus
  buttonText: string
}

export default class BucketOverlayForm extends PureComponent<Props> {
  public render() {
    const {
      name,
      onSubmit,
      ruleType,
      buttonText,
      errorMessage,
      retentionSeconds,
      nameInputStatus,
      onCloseModal,
      onChangeInput,
      onChangeRuleType,
      onChangeRetentionRule,
    } = this.props

    return (
      <Form>
        <Form.Element label="Name" errorMessage={errorMessage}>
          <Input
            placeholder="Give your bucket a name"
            name="name"
            autoFocus={true}
            value={name}
            onChange={onChangeInput}
            status={nameInputStatus}
          />
        </Form.Element>
        <Retention
          type={ruleType}
          retentionSeconds={retentionSeconds}
          onChangeRuleType={onChangeRuleType}
          onChangeRetentionRule={onChangeRetentionRule}
        />
        <Form.Footer>
          <Button
            text="Cancel"
            color={ComponentColor.Danger}
            onClick={onCloseModal}
          />
          <Button
            text={buttonText}
            color={ComponentColor.Primary}
            onClick={onSubmit}
          />
        </Form.Footer>
      </Form>
    )
  }
}

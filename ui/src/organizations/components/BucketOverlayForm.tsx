import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
// Components
import {
  Form,
  Input,
  Button,
  ComponentColor,
  ComponentStatus,
  ButtonType,
  Grid,
} from 'src/clockface'
import Retention from 'src/organizations/components/Retention'

// Types
import {BucketRetentionRules} from 'src/api'

interface Props {
  name: string
  errorMessage: string
  retentionSeconds: number
  ruleType: BucketRetentionRules.TypeEnum
  onSubmit: (e: FormEvent<HTMLFormElement>) => void
  onCloseModal: () => void
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (t: BucketRetentionRules.TypeEnum) => void
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
      <Form onSubmit={onSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column>
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
            </Grid.Column>
            <Grid.Column>
              <Retention
                type={ruleType}
                retentionSeconds={retentionSeconds}
                onChangeRuleType={onChangeRuleType}
                onChangeRetentionRule={onChangeRetentionRule}
              />
            </Grid.Column>
            <Grid.Column>
              <Form.Footer>
                <Button
                  text="Cancel"
                  color={ComponentColor.Danger}
                  onClick={onCloseModal}
                  type={ButtonType.Button}
                />
                <Button
                  text={buttonText}
                  color={ComponentColor.Primary}
                  type={ButtonType.Submit}
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }
}

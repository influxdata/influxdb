// Libraries
import React, {PureComponent} from 'react'

import _ from 'lodash'

// Components
import {
  Button,
  IconFont,
  ComponentColor,
  Alert,
  ComponentSpacer,
  AlignItems,
  FlexDirection,
  ComponentSize,
  ButtonType,
} from '@influxdata/clockface'
import {Form} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onConfirm: () => void
}

@ErrorHandling
class EditOrgConfirmationForm extends PureComponent<Props> {
  public render() {
    const {onConfirm} = this.props

    return (
      <Form onSubmit={onConfirm}>
        <ComponentSpacer
          alignItems={AlignItems.Center}
          direction={FlexDirection.Column}
          margin={ComponentSize.Large}
        >
          <Alert color={ComponentColor.Danger} icon={IconFont.AlertTriangle}>
            Bad things could happen if you don't read this!
          </Alert>
          <Form.Element label="">
            <>
              <p>
                Updating the name of an Organization can have unintended
                consequences. Anything that references this Organization by name
                will stop working including:
              </p>
              <ul>
                <li>Queries</li>
                <li>Dashboards</li>
                <li>Tasks</li>
                <li>Telegraf Configurations</li>
                <li>Client Libraries</li>
              </ul>
            </>
          </Form.Element>
          <ComponentSpacer
            alignItems={AlignItems.Center}
            direction={FlexDirection.Row}
            margin={ComponentSize.Small}
          >
            <Button
              color={ComponentColor.Danger}
              text="I understand, let's rename my Organization"
              type={ButtonType.Submit}
            />
          </ComponentSpacer>
        </ComponentSpacer>
      </Form>
    )
  }
}

export default EditOrgConfirmationForm

// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSpacer,
  FlexDirection,
  AlignItems,
  ComponentSize,
  IconFont,
  ComponentColor,
  Alert,
  JustifyContent,
} from '@influxdata/clockface'

// Constants
import {CLOUD} from 'src/shared/constants'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'
import CheckoutButton from 'src/cloud/components/CheckoutButton'

interface Props {
  resourceName: string
  limitStatus: LimitStatus
}

export default class AssetLimitAlert extends PureComponent<Props> {
  public render() {
    const {limitStatus, resourceName} = this.props
    if (CLOUD && limitStatus === LimitStatus.EXCEEDED) {
      return (
        <ComponentSpacer
          direction={FlexDirection.Column}
          alignItems={AlignItems.Center}
          margin={ComponentSize.Large}
        >
          <Alert icon={IconFont.Cloud} color={ComponentColor.Primary}>
            <ComponentSpacer
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.SpaceBetween}
              margin={ComponentSize.Medium}
            >
              <div>
                {`Hey there, looks like you have reached the maximum number of
              ${resourceName} you can create as part of your plan.`}
                <br />
              </div>
              <CheckoutButton />
            </ComponentSpacer>
          </Alert>
          {this.props.children}
        </ComponentSpacer>
      )
    }
    return this.props.children
  }
}

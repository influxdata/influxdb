// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  FlexBox,
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
  resources: string[]
  limitStatus: LimitStatus
  className?: string
}

export default class RateLimitAlert extends PureComponent<Props> {
  public render() {
    const {limitStatus, className} = this.props

    if (CLOUD && limitStatus === LimitStatus.EXCEEDED) {
      return (
        <FlexBox
          direction={FlexDirection.Column}
          alignItems={AlignItems.Center}
          margin={ComponentSize.Large}
          stretchToFitWidth={true}
          className={className}
        >
          <Alert icon={IconFont.Cloud} color={ComponentColor.Primary}>
            <FlexBox
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.SpaceBetween}
              margin={ComponentSize.Medium}
            >
              <div>
                {this.message}
                <br />
              </div>
              <CheckoutButton />
            </FlexBox>
          </Alert>
        </FlexBox>
      )
    }

    return null
  }

  private get message(): string {
    return `Hey there, it looks like you have exceeded your plan's ${
      this.resourceName
    } limits.${this.additionalMessage}`
  }

  private get additionalMessage(): string {
    if (this.props.resources.includes('cardinality')) {
      return ' Your writes will be rejected until resolved.'
    }

    return ''
  }

  private get resourceName(): string {
    const {resources} = this.props

    const renamedResources = resources.map(resource => {
      if (resource === 'cardinality') {
        return 'total series'
      }

      return resource
    })

    return renamedResources.join(' and ')
  }
}

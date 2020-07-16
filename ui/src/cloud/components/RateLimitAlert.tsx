// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentSize,
  IconFont,
  JustifyContent,
  Gradients,
  InfluxColors,
  BannerPanel,
} from '@influxdata/clockface'

// Utils
import {
  extractRateLimitResources,
  extractRateLimitStatus,
} from 'src/cloud/utils/limits'

// Constants
import {CLOUD} from 'src/shared/constants'

// Types
import {AppState} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'
import CheckoutButton from 'src/cloud/components/CheckoutButton'

interface StateProps {
  resources: string[]
  status: LimitStatus
}
interface OwnProps {
  className?: string
}
type Props = StateProps & OwnProps

class RateLimitAlert extends PureComponent<Props> {
  public render() {
    const {status, className} = this.props

    if (CLOUD && status === LimitStatus.EXCEEDED) {
      return (
        <FlexBox
          direction={FlexDirection.Column}
          alignItems={AlignItems.Center}
          margin={ComponentSize.Large}
          stretchToFitWidth={true}
          className={className}
        >
          <BannerPanel
            size={ComponentSize.ExtraSmall}
            icon={IconFont.Cloud}
            gradient={Gradients.BeijingEclipse}
            textColor={InfluxColors.Twilight}
          >
            <FlexBox justifyContent={JustifyContent.SpaceBetween}>
              {this.message}
              <FlexBox
                direction={FlexDirection.Row}
                justifyContent={JustifyContent.FlexEnd}
                margin={ComponentSize.Medium}
                style={{minWidth: '200px'}}
              >
                <div>Need to write more data?</div>
                <CheckoutButton />
              </FlexBox>
            </FlexBox>
          </BannerPanel>
        </FlexBox>
      )
    }

    return null
  }

  private get message(): JSX.Element {
    return <span>You've reached the maximum {this.resourceName} available in your plan.{this.additionalMessage}</span>
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
        return 'series cardinality'
      }

      return resource
    })

    return renamedResources.join(' and ')
  }
}

const mstp = (state: AppState) => {
  const {
    cloud: {limits},
  } = state

  const resources = extractRateLimitResources(limits)
  const status = extractRateLimitStatus(limits)

  return {
    status,
    resources,
  }
}

export default connect<StateProps>(mstp)(RateLimitAlert)

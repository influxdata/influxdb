// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

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
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

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

interface StateProps {
  resources: string[]
  status: LimitStatus
}
interface OwnProps {
  className?: string
}
type Props = StateProps & OwnProps

const RateLimitAlert: FC<Props> = ({status, className, resources}) => {
  const rateLimitAlertClass = classnames('rate-alert', {
    [`${className}`]: className,
  })

  if (
    CLOUD &&
    status === LimitStatus.EXCEEDED &&
    resources.includes('cardinality')
  ) {
    return (
      <FlexBox
        direction={FlexDirection.Column}
        alignItems={AlignItems.Center}
        margin={ComponentSize.Large}
        className={rateLimitAlertClass}
      >
        <BannerPanel
          size={ComponentSize.ExtraSmall}
          gradient={Gradients.PolarExpress}
          icon={IconFont.Cloud}
          hideMobileIcon={true}
          textColor={InfluxColors.Yeti}
        >
          <div className="rate-alert--content">
            <span>
              You've reached the maximum{' '}
              <a
                href="https://v2.docs.influxdata.com/v2.0/reference/glossary/#series-cardinality"
                target="_blank"
              >
                series cardinality
              </a>{' '}
              available in your plan. Need to write more data?
            </span>
            <FlexBox
              justifyContent={JustifyContent.Center}
              className="rate-alert--button"
            >
              <CloudUpgradeButton />
            </FlexBox>
          </div>
        </BannerPanel>
      </FlexBox>
    )
  }

  if (CLOUD) {
    return <CloudUpgradeButton />
  }

  return null
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

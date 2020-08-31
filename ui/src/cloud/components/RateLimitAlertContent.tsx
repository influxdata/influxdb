// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {get, find} from 'lodash'
import classnames from 'classnames'

// Components
import {
  FlexBox,
  JustifyContent,
  LinkButton,
  ComponentColor,
  ButtonShape,
  ComponentSize,
} from '@influxdata/clockface'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Constants
import {
  HIDE_UPGRADE_CTA_KEY,
  PAID_ORG_HIDE_UPGRADE_SETTING,
} from 'src/cloud/constants'

// Types
import {AppState, OrgSetting} from 'src/types'

interface StateProps {
  showUpgrade: boolean
}

interface OwnProps {
  className?: string
}
type Props = StateProps & OwnProps

const RateLimitAlertContent: FC<Props> = ({showUpgrade, className}) => {
  const rateLimitAlertContentClass = classnames('rate-alert--content', {
    [`${className}`]: className,
  })

  if (showUpgrade) {
    return (
      <div className={`${rateLimitAlertContentClass} rate-alert--content_free`}>
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
    )
  }

  return (
    <div className={`${rateLimitAlertContentClass} rate-alert--content_payg`}>
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
        <LinkButton
          className="contact-support--button"
          color={ComponentColor.Primary}
          size={ComponentSize.Small}
          shape={ButtonShape.Default}
          href="https://support.influxdata.com/s/"
          target="_blank"
          text="Contact Support"
        />
      </FlexBox>
    </div>
  )
}

const mstp = (state: AppState) => {
  const settings = get(state, 'cloud.orgSettings.settings', [])
  const hideUpgradeButtonSetting = find(
    settings,
    (setting: OrgSetting) => setting.key === HIDE_UPGRADE_CTA_KEY
  )
  if (
    !hideUpgradeButtonSetting ||
    hideUpgradeButtonSetting.value !== PAID_ORG_HIDE_UPGRADE_SETTING.value
  ) {
    return {showUpgrade: true}
  }
  return {showUpgrade: false}
}

export default connect<StateProps>(mstp)(RateLimitAlertContent)

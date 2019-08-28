// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import {Page} from 'src/pageLayout'
import Collectors from 'src/telegrafs/components/Collectors'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'
import {FlexBox, FlexDirection, JustifyContent} from '@influxdata/clockface'

// Utils
import {
  extractRateLimitResources,
  extractRateLimitStatus,
} from 'src/cloud/utils/limits'

// Types
import {AppState, Organization} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'

interface StateProps {
  org: Organization
  limitedResources: string[]
  limitStatus: LimitStatus
}

@ErrorHandling
class TelegrafsPage extends PureComponent<StateProps> {
  public render() {
    const {org, children, limitedResources, limitStatus} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <LimitChecker>
            <LoadDataHeader />
            <FlexBox
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.Center}
            >
              {this.isCardinalityExceeded && (
                <RateLimitAlert
                  resources={limitedResources}
                  limitStatus={limitStatus}
                  className="load-data--rate-alert"
                />
              )}
            </FlexBox>
            <LoadDataTabbedPage activeTab="telegrafs" orgID={org.id}>
              <GetResources resource={ResourceTypes.Buckets}>
                <GetResources resource={ResourceTypes.Telegrafs}>
                  <Collectors />
                </GetResources>
              </GetResources>
            </LoadDataTabbedPage>
          </LimitChecker>
        </Page>
        {children}
      </>
    )
  }
  private get isCardinalityExceeded(): boolean {
    const {limitedResources} = this.props

    return limitedResources.includes('cardinality')
  }
}

const mstp = ({orgs: {org}, cloud: {limits}}: AppState) => {
  const limitedResources = extractRateLimitResources(limits)
  const limitStatus = extractRateLimitStatus(limits)

  return {org, limitedResources, limitStatus}
}

export default connect<StateProps>(mstp)(TelegrafsPage)

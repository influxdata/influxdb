// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {Route, Switch} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import Collectors from 'src/telegrafs/components/Collectors'
import GetResources from 'src/resources/components/GetResources'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'
import TelegrafInstructionsOverlay from 'src/telegrafs/components/TelegrafInstructionsOverlay'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import {
  FlexBox,
  FlexDirection,
  JustifyContent,
  Page,
} from '@influxdata/clockface'
import OverlayHandler, {
  RouteOverlay,
} from 'src/overlays/components/RouteOverlay'

const TelegrafConfigOverlay = RouteOverlay(
  OverlayHandler,
  'telegraf-config',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
  }
)
const TelegrafOutputOverlay = RouteOverlay(
  OverlayHandler,
  'telegraf-output',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
  }
)

// Utils
import {extractRateLimitResources} from 'src/cloud/utils/limits'
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Constant
import {ORGS, ORG_ID, TELEGRAFS} from 'src/shared/constants/routes'

interface StateProps {
  org: Organization
  limitedResources: string[]
}

const telegrafsPath = `/${ORGS}/${ORG_ID}/load-data/${TELEGRAFS}`

@ErrorHandling
class TelegrafsPage extends PureComponent<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Telegraf', 'Load Data'])}>
          <LimitChecker>
            <LoadDataHeader />
            <FlexBox
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.Center}
            >
              {this.isCardinalityExceeded && (
                <RateLimitAlert className="load-data--rate-alert" />
              )}
            </FlexBox>
            <LoadDataTabbedPage activeTab="telegrafs" orgID={org.id}>
              <GetResources
                resources={[ResourceType.Buckets, ResourceType.Telegrafs]}
              >
                <Collectors />
              </GetResources>
            </LoadDataTabbedPage>
          </LimitChecker>
        </Page>
        <Switch>
          <Route
            path={`${telegrafsPath}/:id/view`}
            component={TelegrafConfigOverlay}
          />
          <Route
            path={`${telegrafsPath}/:id/instructions`}
            component={TelegrafInstructionsOverlay}
          />
          <Route
            path={`${telegrafsPath}/output`}
            component={TelegrafOutputOverlay}
          />
          <Route path={`${telegrafsPath}/new`} component={CollectorsWizard} />
        </Switch>
      </>
    )
  }
  private get isCardinalityExceeded(): boolean {
    const {limitedResources} = this.props

    return limitedResources.includes('cardinality')
  }
}

const mstp = (state: AppState) => {
  const org = getOrg(state)
  const {
    cloud: {limits},
  } = state
  const limitedResources = extractRateLimitResources(limits)

  return {org, limitedResources}
}

export default connect<StateProps>(mstp)(TelegrafsPage)

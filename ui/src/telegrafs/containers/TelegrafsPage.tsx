// Libraries
import React, {PureComponent} from 'react'
import {Route, Switch} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import Collectors from 'src/telegrafs/components/Collectors'
import GetResources from 'src/resources/components/GetResources'
import LimitChecker from 'src/cloud/components/LimitChecker'
import TelegrafInstructionsOverlay from 'src/telegrafs/components/TelegrafInstructionsOverlay'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import {Page} from '@influxdata/clockface'
import OverlayHandler, {
  RouteOverlay,
} from 'src/overlays/components/RouteOverlay'

const TelegrafConfigOverlay = RouteOverlay(
  OverlayHandler as any,
  'telegraf-config',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
  }
)
const TelegrafOutputOverlay = RouteOverlay(
  OverlayHandler as any,
  'telegraf-output',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
  }
)

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {ResourceType} from 'src/types'

// Constant
import {ORGS, ORG_ID, TELEGRAFS} from 'src/shared/constants/routes'

const telegrafsPath = `/${ORGS}/${ORG_ID}/load-data/${TELEGRAFS}`

@ErrorHandling
class TelegrafsPage extends PureComponent {
  public render() {
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Telegraf', 'Load Data'])}>
          <LimitChecker>
            <LoadDataHeader />
            <LoadDataTabbedPage activeTab="telegrafs">
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
}

export default TelegrafsPage

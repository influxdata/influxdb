// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Page} from 'src/pageLayout'
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import TabbedPage from 'src/shared/components/tabbed_page/TabbedPage'
import Settings from 'src/me/components/account/Settings'
import Telegrafs from 'src/configuration/components/Telegrafs'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

interface OwnProps {
  activeTabUrl: string
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class ConfigurationPage extends Component<Props> {
  public render() {
    const {
      params: {tab},
    } = this.props

    return (
      <Page titleTag="Configuration">
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <PageTitleWithOrg title="Configuration" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <GetResources resource={ResourceTypes.Authorizations}>
              <CloudExclude>
                <TabbedPage
                  name="Configuration"
                  parentUrl="/configuration"
                  activeTabUrl={tab}
                >
                  <TabbedPageSection
                    id="telegrafs_tab"
                    url="telegrafs_tab"
                    title="Telegraf"
                  >
                    <GetResources resource={ResourceTypes.Buckets}>
                      <GetResources resource={ResourceTypes.Telegrafs}>
                        <Telegrafs />
                      </GetResources>
                    </GetResources>
                  </TabbedPageSection>
                  <TabbedPageSection
                    id="scrapers_tab"
                    url="scrapers_tab"
                    title="Scrapers"
                  />
                  <TabbedPageSection
                    id="settings_tab"
                    url="settings_tab"
                    title="Profile"
                  >
                    <Settings />
                  </TabbedPageSection>
                </TabbedPage>
              </CloudExclude>
              <CloudOnly>
                <TabbedPage
                  name="Configuration"
                  parentUrl="/configuration"
                  activeTabUrl={tab}
                >
                  <TabbedPageSection
                    id="settings_tab"
                    url="settings_tab"
                    title="Profile"
                  >
                    <Settings />
                  </TabbedPageSection>
                </TabbedPage>
              </CloudOnly>
            </GetResources>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

export default withRouter<OwnProps>(ConfigurationPage)

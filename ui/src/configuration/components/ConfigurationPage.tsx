// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Page} from 'src/pageLayout'
import GetLabels from 'src/configuration/components/GetLabels'
import {Spinner} from 'src/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import TabbedPage from 'src/shared/components/tabbed_page/TabbedPage'
import Labels from 'src/organizations/components/Labels'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

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
            <Page.Title title="Configuration" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <TabbedPage
              name={'Configuration'}
              parentUrl={`/configuration`}
              activeTabUrl={tab}
            >
              <TabbedPageSection
                id="labels_tab"
                url="labels_tab"
                title="Labels"
              >
                <GetLabels>
                  {(labels, loading) => (
                    <Spinner loading={loading}>
                      <Labels labels={labels} />
                    </Spinner>
                  )}
                </GetLabels>
              </TabbedPageSection>
            </TabbedPage>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

export default withRouter<OwnProps>(ConfigurationPage)

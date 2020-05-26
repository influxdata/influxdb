// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import CompletionAdvancedButton from 'src/onboarding/components/CompletionAdvancedButton'
import CompletionQuickStartButton from 'src/onboarding/components/CompletionQuickStartButton'

// Constants
import {
  QuickstartScraperCreationSuccess,
  QuickstartScraperCreationError,
  QuickstartDashboardCreationSuccess,
  QuickstartDashboardCreationError,
} from 'src/shared/copy/notifications'
import {ossMetricsTemplate} from 'src/templates/constants/defaultTemplates'

// APIs
import {getDashboards} from 'src/organizations/apis'
import {client} from 'src/utils/api'
import {createDashboardFromTemplate as createDashboardFromTemplateAJAX} from 'src/templates/api'

// Types
import {
  Button,
  ComponentColor,
  ComponentSize,
  Columns,
  Grid,
  DapperScrollbars,
} from '@influxdata/clockface'
import {Dashboard} from 'src/types'
import {ScraperTargetRequest} from '@influxdata/influx'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {QUICKSTART_SCRAPER_TARGET_URL} from 'src/dataLoaders/constants/pluginConfigs'

interface Props extends OnboardingStepProps {
  orgID: string
  bucketID: string
}

@ErrorHandling
class CompletionStep extends PureComponent<Props> {
  public componentDidMount() {
    window.addEventListener('keydown', this.handleKeydown)
  }

  public componentWillUnmount() {
    window.removeEventListener('keydown', this.handleKeydown)
  }

  public render() {
    const {onExit} = this.props

    return (
      <div className="onboarding-step">
        <div className="wizard-step--scroll-area">
          <DapperScrollbars autoHide={false}>
            <div className="wizard-step--scroll-content">
              <h3 className="wizard-step--title">You are ready to go!</h3>
              <h5 className="wizard-step--sub-title">
                Your InfluxDB 2.0 has 1 organization, 1 user, and 1 bucket.
              </h5>
              <div className="splash-logo secondary" />
              <h3 className="wizard-step--title">
                Let’s start collecting data!
              </h3>
              <dl className="wizard-completion--options">
                <Grid>
                  <Grid.Row>
                    <Grid.Column
                      widthXS={Columns.Twelve}
                      widthSM={Columns.Four}
                    >
                      <div className="wizard-completion--option">
                        <ResourceFetcher<Dashboard[]> fetcher={getDashboards}>
                          {dashboards => (
                            <CompletionQuickStartButton
                              onExit={this.handleQuickStart}
                              dashboards={dashboards}
                            />
                          )}
                        </ResourceFetcher>
                        <dt>Timing is everything!</dt>
                        <dd>
                          This will set up local metric collection and allow you
                          to explore the features of InfluxDB 2.0 quickly.
                        </dd>
                      </div>
                    </Grid.Column>
                    <Grid.Column
                      widthXS={Columns.Twelve}
                      widthSM={Columns.Four}
                    >
                      <div className="wizard-completion--option">
                        <CompletionAdvancedButton onExit={onExit} />
                        <dt>Whoa looks like you’re an expert!</dt>
                        <dd>
                          This allows you to set up Telegraf, scrapers, and much
                          more.
                        </dd>
                      </div>
                    </Grid.Column>
                    <Grid.Column
                      widthXS={Columns.Twelve}
                      widthSM={Columns.Four}
                    >
                      <div className="wizard-completion--option">
                        <Button
                          text="Configure Later"
                          color={ComponentColor.Success}
                          size={ComponentSize.Large}
                          onClick={onExit}
                          testID="button--conf-later"
                        />
                        <dt>I've got this...</dt>
                        <dd>
                          Jump into InfluxDB 2.0 and set up data collection when
                          you’re ready.
                        </dd>
                      </div>
                    </Grid.Column>
                  </Grid.Row>
                </Grid>
              </dl>
              <h5 className="wizard-step--sub-title" />
            </div>
          </DapperScrollbars>
        </div>
      </div>
    )
  }

  private handleQuickStart = async () => {
    try {
      await client.scrapers.create({
        name: 'new target',
        type: ScraperTargetRequest.TypeEnum.Prometheus,
        url: QUICKSTART_SCRAPER_TARGET_URL,
        bucketID: this.props.bucketID,
        orgID: this.props.orgID,
      })
      this.props.notify(QuickstartScraperCreationSuccess)
    } catch (err) {
      this.props.notify(QuickstartScraperCreationError)
    }
    try {
      await createDashboardFromTemplateAJAX(
        ossMetricsTemplate(),
        this.props.orgID
      )
      this.props.notify(QuickstartDashboardCreationSuccess)
    } catch (err) {
      this.props.notify(QuickstartDashboardCreationError)
    }
    this.props.onExit()
  }

  private handleKeydown = (e: KeyboardEvent): void => {
    if (e.key === 'Enter') {
      this.props.onExit()
    }
  }
}

export default CompletionStep

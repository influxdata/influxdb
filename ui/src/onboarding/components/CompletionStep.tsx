// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import CompletionAdvancedButton from 'src/onboarding/components/CompletionAdvancedButton'
import CompletionQuickStartButton from 'src/onboarding/components/CompletionQuickStartButton'

// APIs
import {getOrganizations, getDashboards} from 'src/organizations/apis'

// Types
import {
  Button,
  ComponentColor,
  ComponentSize,
  Grid,
  Columns,
} from 'src/clockface'
import {Organization, Dashboard} from 'src/api'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

@ErrorHandling
class CompletionStep extends PureComponent<OnboardingStepProps> {
  public componentDidMount() {
    window.addEventListener('keydown', this.handleKeydown)
  }

  public componentWillUnmount() {
    window.removeEventListener('keydown', this.handleKeydown)
  }

  public render() {
    const {onExit} = this.props

    return (
      <div className="wizard--bookend-step">
        <h3 className="wizard-step--title">You are ready to go!</h3>
        <h5 className="wizard-step--sub-title">
          Your InfluxDB 2.0 has 1 organization, 1 user, and 1 bucket.
        </h5>
        <div className="splash-logo secondary" />
        <h3 className="wizard-step--title">Let’s start collecting data!</h3>
        <dl className="wizard-completion--options">
          <Grid>
            <Grid.Row>
              <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Four}>
                <div className="wizard-completion--option">
                  <ResourceFetcher<Dashboard[]> fetcher={getDashboards}>
                    {dashboards => (
                      <CompletionQuickStartButton dashboards={dashboards} />
                    )}
                  </ResourceFetcher>
                  <dt>Time is of the essence!</dt>
                  <dd>
                    This will set up local metric collection and allow you to
                    explore the features of InfluxDB 2.0 quickly.
                  </dd>
                </div>
              </Grid.Column>
              <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Four}>
                <div className="wizard-completion--option">
                  <ResourceFetcher<Organization[]> fetcher={getOrganizations}>
                    {orgs => <CompletionAdvancedButton orgs={orgs} />}
                  </ResourceFetcher>
                  <dt>Whoa looks like you’re an expert!</dt>
                  <dd>
                    This allows you to set up Telegraf, scrapers, and much more.
                  </dd>
                </div>
              </Grid.Column>
              <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Four}>
                <div className="wizard-completion--option">
                  <Button
                    text="Configure Later"
                    color={ComponentColor.Success}
                    size={ComponentSize.Large}
                    onClick={onExit}
                  />
                  <dt>I've got this...</dt>
                  <dd>
                    Jump into Influx 2.0 and set up data collection when you’re
                    ready.
                  </dd>
                </div>
              </Grid.Column>
            </Grid.Row>
          </Grid>
        </dl>
        <h5 className="wizard-step--sub-title" />
      </div>
    )
  }

  private handleKeydown = (e: KeyboardEvent): void => {
    if (e.key === 'Enter') {
      this.props.onExit()
    }
  }
}

export default CompletionStep

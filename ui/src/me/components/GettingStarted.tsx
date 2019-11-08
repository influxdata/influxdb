// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {
  Panel,
  Button,
  Gradients,
  InfluxColors,
  ComponentColor,
  ComponentSize,
} from '@influxdata/clockface'
import DashboardingGraphic from 'src/me/graphics/DashboardingGraphic'
import ExploreGraphic from 'src/me/graphics/ExploreGraphic'
import CollectorGraphic from 'src/me/graphics/CollectorGraphic'

const GettingStarted: FunctionComponent<WithRouterProps> = ({
  params: {orgID},
  router,
}) => {
  const handleLoadDataClick = (): void => {
    router.push(`/orgs/${orgID}/load-data/telegrafs`)
  }
  const handleDashboardsClick = (): void => {
    router.push(`/orgs/${orgID}/dashboards`)
  }
  const handleAlertsClick = (): void => {
    router.push(`/orgs/${orgID}/alerting`)
  }

  return (
    <div className="getting-started">
      <Panel
        className="getting-started--card highlighted"
        gradient={Gradients.PolarExpress}
      >
        <div className="getting-started--card-digit">1</div>
        <Panel.Body>
          <CollectorGraphic />
        </Panel.Body>
        <Panel.Footer>
          <Button
            text="Load your data"
            color={ComponentColor.Primary}
            size={ComponentSize.Large}
            onClick={handleLoadDataClick}
          />
        </Panel.Footer>
      </Panel>
      <Panel
        className="getting-started--card"
        backgroundColor={InfluxColors.Onyx}
      >
        <div className="getting-started--card-digit">2</div>
        <Panel.Body>
          <DashboardingGraphic />
        </Panel.Body>
        <Panel.Footer>
          <Button
            text="Build a dashboard"
            color={ComponentColor.Primary}
            size={ComponentSize.Large}
            onClick={handleDashboardsClick}
          />
        </Panel.Footer>
      </Panel>
      <Panel
        className="getting-started--card"
        backgroundColor={InfluxColors.Onyx}
      >
        <div className="getting-started--card-digit">3</div>
        <Panel.Body>
          <ExploreGraphic />
        </Panel.Body>
        <Panel.Footer>
          <Button
            text="Set up alerting"
            color={ComponentColor.Primary}
            size={ComponentSize.Large}
            onClick={handleAlertsClick}
          />
        </Panel.Footer>
      </Panel>
    </div>
  )
}

export default withRouter<{}>(GettingStarted)

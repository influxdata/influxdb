// Libraries
import React, {PureComponent} from 'react'
import {withRouter, Link, WithRouterProps} from 'react-router'

// Components
import GradientBorder from 'src/shared/components/cells/GradientBorder'
import DashboardingGraphic from 'src/me/graphics/DashboardingGraphic'
import ExploreGraphic from 'src/me/graphics/ExploreGraphic'
import CollectorGraphic from 'src/me/graphics/CollectorGraphic'

class GettingStarted extends PureComponent<WithRouterProps> {
  public render() {
    const {
      params: {orgID},
    } = this.props
    return (
      <div className="getting-started">
        <div className="getting-started--container">
          <Link
            to={`/orgs/${orgID}/load-data/telegrafs`}
            className="getting-started--card"
          >
            <GradientBorder />
            <CollectorGraphic />
            <h3 className="getting-started--title">
              Configure a<br />
              Data Collector
            </h3>
          </Link>
        </div>
        <div className="getting-started--container">
          <Link
            to={`/orgs/${orgID}/dashboards`}
            className="getting-started--card"
          >
            <GradientBorder />
            <DashboardingGraphic />
            <h3 className="getting-started--title">
              Build a Monitoring
              <br />
              Dashboard
            </h3>
          </Link>
        </div>
        <div className="getting-started--container">
          <Link
            to={`/orgs/${orgID}/data-explorer`}
            className="getting-started--card"
          >
            <GradientBorder />
            <ExploreGraphic />
            <h3 className="getting-started--title">
              Explore your data
              <br />
              using Flux
            </h3>
          </Link>
        </div>
      </div>
    )
  }
}

export default withRouter<{}>(GettingStarted)

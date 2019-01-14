// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import GradientBorder from 'src/shared/components/cells/GradientBorder'
import DashboardingGraphic from 'src/me/graphics/DashboardingGraphic'

// Styles
import 'src/me/components/GettingStarted.scss'

export default class GettingStarted extends PureComponent {
  public render() {
    return (
      <div className="getting-started">
        <div className="getting-started--container">
          <Link to={`/data-explorer`} className="getting-started--card">
            <GradientBorder />
            <div className="getting-started--image" />
            <h3 className="getting-started--title">
              Configure a<br />
              Data Collector
            </h3>
          </Link>
        </div>
        <div className="getting-started--container">
          <Link to={`/dashboards`} className="getting-started--card">
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
          <Link to={`/data-explorer`} className="getting-started--card">
            <GradientBorder />
            <div className="getting-started--image" />
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

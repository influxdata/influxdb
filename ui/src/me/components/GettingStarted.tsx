// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Components
import GradientBorder from 'src/shared/components/cells/GradientBorder'
import DashboardingGraphic from 'src/me/graphics/DashboardingGraphic'
import ExploreGraphic from 'src/me/graphics/ExploreGraphic'
import CollectorGraphic from 'src/me/graphics/CollectorGraphic'

// Types
import {Organization} from 'src/types'

interface StateProps {
  orgs: Organization[]
}

class GettingStarted extends PureComponent<StateProps> {
  public render() {
    return (
      <div className="getting-started">
        <div className="getting-started--container">
          <Link
            to={this.firstOrgCollectorLink}
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

  private get firstOrgCollectorLink(): string {
    const {orgs} = this.props
    const firstOrgID = orgs[0].id

    return `/organizations/${firstOrgID}/telegrafs`
  }
}

const mstp = (state): StateProps => {
  const {orgs} = state

  return {orgs}
}

export default connect<StateProps>(
  mstp,
  null
)(GettingStarted)

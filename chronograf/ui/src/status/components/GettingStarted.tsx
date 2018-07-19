import React, {Component} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class GettingStarted extends Component {
  public render() {
    return (
      <FancyScrollbar className="getting-started--container">
        <div className="getting-started">
          <div className="getting-started--cell intro">
            <h5>
              <span className="icon cubo-uniform" /> Welcome to Chronograf!
            </h5>
            <p>Follow the links below to explore Chronograf’s features.</p>
          </div>
          <div className="getting-started--cell">
            <p>
              <strong>Install the TICK Stack</strong>
              <br />Save some time and use this handy tool to install the rest
              of the stack:
            </p>
            <p>
              <a href="https://github.com/influxdata/sandbox" target="_blank">
                <span className="icon github" /> TICK Sandbox
              </a>
            </p>
          </div>
          <div className="getting-started--cell">
            <p>
              <strong>Guides</strong>
            </p>
            <p>
              <a
                href="https://docs.influxdata.com/chronograf/latest/guides/create-a-dashboard/"
                target="_blank"
              >
                Create a Dashboard
              </a>
              <br />
              <a
                href="https://docs.influxdata.com/chronograf/latest/guides/create-a-kapacitor-alert/"
                target="_blank"
              >
                Create a Kapacitor Alert
              </a>
              <br />
              <a
                href="https://docs.influxdata.com/chronograf/latest/guides/configure-kapacitor-event-handlers/"
                target="_blank"
              >
                Configure Kapacitor Event Handlers
              </a>
              <br />
              <a
                href="https://docs.influxdata.com/chronograf/latest/guides/transition-web-admin-interface/"
                target="_blank"
              >
                Transition from InfluxDB's Web Admin Interface
              </a>
              <br />
              <a
                href="https://docs.influxdata.com/chronograf/latest/guides/dashboard-template-variables/"
                target="_blank"
              >
                Dashboard Template Variables
              </a>
              <br />
              <a
                href="https://docs.influxdata.com/chronograf/latest/guides/advanced-kapacitor/"
                target="_blank"
              >
                Advanced Kapacitor Usage
              </a>
            </p>
          </div>
          <div className="getting-started--cell">
            <p>
              <strong>Questions & Comments</strong>
            </p>
            <p>
              If you have any product feedback please open a GitHub issue and
              we'll take a look. For any questions or other issues try posting
              on our&nbsp;
              <a href="https://community.influxdata.com/" target="_blank">
                Community Forum
              </a>.
            </p>
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

export default GettingStarted

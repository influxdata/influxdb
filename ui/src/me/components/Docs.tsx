// Libraries
import React, {PureComponent} from 'react'

// Components
import {Panel} from '@influxdata/clockface'

const supportLinks = [
  {
    link: 'https://docs.influxdata.com/influxdb/latest/query-data/get-started/',
    title: 'Get Started with Flux',
  },
  {
    link:
      'https://docs.influxdata.com/influxdb/latest/query-data/execute-queries/data-explorer/',
    title: 'Explore Metrics',
  },
  {
    link:
      'https://docs.influxdata.com/influxdb/latest/visualize-data/dashboards/',
    title: 'Build a Dashboard',
  },
  {
    link:
      'https://docs.influxdata.com/influxdb/latest/process-data/get-started/',
    title: 'Write a Task',
  },
]

export default class SupportLinks extends PureComponent {
  public render() {
    return (
      <Panel>
        <Panel.Header>
          <h4>Some Handy Guides and Tutorials</h4>
        </Panel.Header>
        <Panel.Body>
          <ul className="tutorials-list">
            {supportLinks.map(({link, title}) => (
              <li key={title}>
                <a href={link} target="_blank">
                  {title}
                </a>
              </li>
            ))}
          </ul>
        </Panel.Body>
      </Panel>
    )
  }
}

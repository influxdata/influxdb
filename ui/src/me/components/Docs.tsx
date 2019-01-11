// Libraries
import React, {PureComponent} from 'react'

// Components
import {Panel} from 'src/clockface'

const supportLinks = [
  {
    link: 'https://docs.influxdata.com/',
    title: 'Managing Organizations & Members',
  },
  {
    link: 'https://docs.influxdata.com/',
    title: 'Importing & Exporting Dashboards',
  },
  {
    link: 'https://docs.influxdata.com/',
    title: 'Writing Tasks',
  },
  {
    link: 'https://docs.influxdata.com/',
    title: 'What are Swoggelz?',
  },
]

export default class SupportLinks extends PureComponent {
  public render() {
    return (
      <Panel>
        <Panel.Header title="Some Handy Guides and Tutorials" />
        <Panel.Body>
          <ul className="link-list tutorials">
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

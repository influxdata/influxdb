// Libraries
import React, {PureComponent} from 'react'

const supportLinks = [
  {link: 'https://v2.docs.influxdata.com/v2.0/', title: '📜 Documentation'},
  {link: 'https://community.influxdata.com', title: '💭 Community Forum'},
  {
    link:
      'https://github.com/influxdata/influxdb/issues/new?template=feature_request.md',
    title: '✨ Feature Requests',
  },
  {
    link:
      'https://github.com/influxdata/influxdb/issues/new?template=bug_report.md',
    title: '🐛 Report a bug',
  },
]

export default class SupportLinks extends PureComponent {
  public render() {
    return (
      <ul className="useful-links">
        {supportLinks.map(({link, title}) => (
          <li key={title}>
            <a href={link} target="_blank">
              {title}
            </a>
          </li>
        ))}
      </ul>
    )
  }
}

// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import moment from 'moment'

// Types
import {Organization} from 'src/types/v2'

// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'

interface PassedProps {
  name: () => JSX.Element
  description?: () => JSX.Element
  updatedAt?: string
  owner?: Organization
  labels?: () => JSX.Element
  metadata?: () => JSX.Element[]
  contextMenu?: () => JSX.Element
  children?: JSX.Element[] | JSX.Element
}

interface DefaultProps {
  testID?: string
}

type Props = PassedProps & DefaultProps

export default class ResourceListCard extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    testID: 'resource-card',
  }

  public render() {
    const {description, children, testID, labels} = this.props

    return (
      <div className="resource-list--card" data-testid={testID}>
        {this.nameAndMeta}
        {description()}
        {labels()}
        {children}
        {this.contextMenu}
      </div>
    )
  }

  private get nameAndMeta(): JSX.Element {
    const {name, updatedAt, owner, metadata} = this.props

    let metaInformation

    if (!updatedAt && !owner && !metadata) {
      metaInformation = null
    } else {
      const metaChildren = React.Children.map(metadata(), (m: JSX.Element) => {
        if (m !== null && m !== undefined) {
          return <div className="resource-list--meta-item">{m}</div>
        }
      })

      metaInformation = (
        <div className="resource-list--meta">
          {this.ownerLink}
          {this.updated}
          {metaChildren}
        </div>
      )
    }

    return (
      <div className="resource-list--name-meta">
        {name()}
        {metaInformation}
      </div>
    )
  }

  private get ownerLink(): JSX.Element {
    const {owner} = this.props

    if (owner) {
      return (
        <div className="resource-list--meta-item">
          <Link
            to={`/organizations/${owner.id}/members_tab`}
            className="resource-list--owner"
          >
            {owner.name}
          </Link>
        </div>
      )
    }
  }

  private get updated(): JSX.Element {
    const {updatedAt} = this.props

    if (updatedAt) {
      const relativeTimestamp = moment(updatedAt).fromNow()
      const absoluteTimestamp = moment(updatedAt).format(UPDATED_AT_TIME_FORMAT)

      return (
        <div className="resource-list--meta-item" title={absoluteTimestamp}>
          {`Modified ${relativeTimestamp}`}
        </div>
      )
    }
  }

  private get contextMenu(): JSX.Element {
    const {contextMenu} = this.props

    if (contextMenu) {
      return <div className="resource-list--context-menu">{contextMenu()}</div>
    }
  }
}

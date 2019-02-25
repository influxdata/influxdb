// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import moment from 'moment'

// Types
import {Organization} from 'src/types/v2'

// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'

interface Props {
  name: () => JSX.Element
  description?: () => JSX.Element
  updatedAt?: string
  owner?: Organization
  labels?: () => JSX.Element
  meta1?: () => JSX.Element
  meta2?: () => JSX.Element
  contextMenu?: () => JSX.Element
  children?: JSX.Element[] | JSX.Element
  testID?: string
}

export default class ResourceListCard extends PureComponent<Props> {
  public render() {
    const {description, children, testID, labels} = this.props

    return (
      <div className="resource-list--card" data-test-id={testID}>
        {this.nameAndMeta}
        {description()}
        {labels()}
        {children}
        {this.contextMenu}
      </div>
    )
  }

  private get nameAndMeta(): JSX.Element {
    const {name} = this.props

    return (
      <div className="resource-list--name-meta">
        {name()}
        {this.meta}
      </div>
    )
  }

  private get meta(): JSX.Element {
    const {updatedAt, owner, meta1, meta2} = this.props

    if (!updatedAt && !owner && !meta1 && !meta2) {
      return null
    }

    return (
      <div className="resource-list--meta">
        {this.ownerLink}
        {this.updated}
        {this.meta1}
        {this.meta2}
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

  private get meta1(): JSX.Element {
    const {meta1} = this.props

    if (meta1) {
      return <div className="resource-list--meta-item">{meta1()}</div>
    }
  }

  private get meta2(): JSX.Element {
    const {meta2} = this.props

    if (meta2) {
      return <div className="resource-list--meta-item">{meta2()}</div>
    }
  }

  private get contextMenu(): JSX.Element {
    const {contextMenu} = this.props

    if (contextMenu) {
      return <div className="resource-list--context-menu">{contextMenu()}</div>
    }
  }
}

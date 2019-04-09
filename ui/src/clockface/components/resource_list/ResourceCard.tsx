// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import moment from 'moment'
import classnames from 'classnames'

// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'

interface Props {
  name: () => JSX.Element
  updatedAt?: string
  owner?: {id: string; name: string}
  children?: JSX.Element[] | JSX.Element
  disabled?: boolean
  testID: string
  description: () => JSX.Element
  labels: () => JSX.Element
  metaData: () => JSX.Element[]
  contextMenu: () => JSX.Element
  toggle: () => JSX.Element
}

export default class ResourceListCard extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'resource-card',
    description: () => null,
    labels: () => null,
    metaData: () => null,
    contextMenu: () => null,
    toggle: () => null,
  }

  public render() {
    const {description, labels, children, testID, toggle} = this.props

    if (toggle()) {
      return (
        <div className={this.className} data-testid={testID}>
          {this.toggle}
          <div className="resource-list--card-contents">
            {this.nameAndMeta}
            {description()}
            {labels()}
            {children}
          </div>
          {this.contextMenu}
        </div>
      )
    }

    return (
      <div className={this.className} data-testid={testID}>
        {this.toggle}
        {this.nameAndMeta}
        {description()}
        {labels()}
        {children}
        {this.contextMenu}
      </div>
    )
  }

  private get className(): string {
    const {disabled, toggle} = this.props
    return classnames('resource-list--card', {
      'resource-list--card__disabled': disabled,
      'resource-list--card__toggleable': toggle() !== null,
    })
  }

  private get toggle(): JSX.Element {
    const {toggle} = this.props

    if (toggle() === null) {
      return
    }

    return <div className="resource-list--toggle">{toggle()}</div>
  }

  private get nameAndMeta(): JSX.Element {
    const {name} = this.props

    return (
      <div className="resource-list--name-meta">
        {name()}
        {this.formattedMetaData}
      </div>
    )
  }

  private get formattedMetaData(): JSX.Element {
    const {updatedAt, owner, metaData} = this.props

    if (!updatedAt && !owner && !metaData) {
      return null
    }

    return (
      <div className="resource-list--meta">
        {this.ownerLink}
        {this.updated}
        {this.metaData}
      </div>
    )
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

  private get ownerLink(): JSX.Element {
    const {owner} = this.props

    if (owner) {
      return (
        <div className="resource-list--meta-item">
          <Link
            to={`/orgs/${owner.id}/members`}
            className="resource-list--owner"
          >
            {owner.name}
          </Link>
        </div>
      )
    }
  }

  private get metaData(): JSX.Element[] {
    const {metaData} = this.props

    if (metaData()) {
      return React.Children.map(metaData(), (m: JSX.Element) => {
        if (m !== null && m !== undefined) {
          return <div className="resource-list--meta-item">{m}</div>
        }
      })
    }
  }

  private get contextMenu(): JSX.Element {
    const {contextMenu} = this.props

    if (contextMenu()) {
      return <div className="resource-list--context-menu">{contextMenu()}</div>
    }
  }
}

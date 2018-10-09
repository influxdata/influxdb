import _ from 'lodash'
import React, {PureComponent} from 'react'
import {Source} from 'src/types/v2'
import {Namespace} from 'src/types'

import {
  Dropdown,
  IconFont,
  DropdownMode,
  Button,
  ButtonShape,
} from 'src/clockface'
import {Page} from 'src/pageLayout'
// import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import LiveUpdatingStatus from 'src/logs/components/LiveUpdatingStatus'

interface SourceItem {
  id: string
  text: string
}

interface Props {
  currentNamespace: Namespace
  availableSources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  onChooseSource: (sourceID: string) => void
  onChooseNamespace: (namespace: Namespace) => void
  liveUpdating: boolean
  onChangeLiveUpdatingStatus: () => void
  onShowOptionsOverlay: () => void
}

const EMPTY_ID = 'none'

class LogsHeader extends PureComponent<Props> {
  public render(): JSX.Element {
    const {
      liveUpdating,
      onChangeLiveUpdatingStatus,
      onShowOptionsOverlay,
    } = this.props

    return (
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <LiveUpdatingStatus
            onChangeLiveUpdatingStatus={onChangeLiveUpdatingStatus}
            liveUpdating={liveUpdating}
          />
          <Page.Title title="Log Viewer" />
        </Page.Header.Left>
        <Page.Header.Right>
          <Dropdown
            customClass="dropdown-300"
            selectedID={this.selectedSource}
            onChange={this.handleChooseSource}
            titleText="Sources"
          >
            {this.sourceDropDownItems}
          </Dropdown>
          <Dropdown
            customClass="dropdown-180"
            icon={IconFont.Disks}
            selectedID={this.selectedNamespace}
            onChange={this.handleChooseNamespace}
            titleText="Namespaces"
            mode={DropdownMode.ActionList}
          >
            {this.namespaceDropDownItems}
          </Dropdown>
          {/* <Authorized requiredRole={EDITOR_ROLE}> */}
          <Button
            onClick={onShowOptionsOverlay}
            shape={ButtonShape.Square}
            icon={IconFont.CogThick}
          />
          {/* </Authorized> */}
        </Page.Header.Right>
      </Page.Header>
    )
  }

  private handleChooseSource = (item: SourceItem) => {
    this.props.onChooseSource(item.id)
  }

  private handleChooseNamespace = (namespace: Namespace) => {
    this.props.onChooseNamespace(namespace)
  }

  private get selectedSource(): string {
    const {availableSources} = this.props
    if (_.isEmpty(availableSources)) {
      return EMPTY_ID
    }

    const id = _.get(this.props, 'currentSource.id', null)

    if (id === null) {
      return availableSources[0].id
    }

    const currentItem = _.find(availableSources, s => s.id === id)

    if (currentItem) {
      return currentItem.id
    }

    return EMPTY_ID
  }

  private get selectedNamespace(): string {
    const {currentNamespace} = this.props

    if (!currentNamespace) {
      return EMPTY_ID
    }

    return `${currentNamespace.database}.${currentNamespace.retentionPolicy}`
  }

  private get namespaceDropDownItems() {
    const {currentNamespaces} = this.props

    if (_.isEmpty(currentNamespaces)) {
      return this.renderEmptyDropdown('No Namespaces Found')
    }

    return currentNamespaces.map((namespace: Namespace) => {
      const namespaceText = `${namespace.database}.${namespace.retentionPolicy}`
      return (
        <Dropdown.Item value={namespace} key={namespaceText} id={namespaceText}>
          {namespaceText}
        </Dropdown.Item>
      )
    })
  }

  private get sourceDropDownItems(): JSX.Element[] {
    const {availableSources} = this.props

    if (_.isEmpty(availableSources)) {
      return this.renderEmptyDropdown('No Sources Found')
    }

    return availableSources.map(source => {
      const sourceText = `${source.name} @ ${source.url}`
      return (
        <Dropdown.Item value={source} id={source.id} key={source.id}>
          {sourceText}
        </Dropdown.Item>
      )
    })
  }

  private renderEmptyDropdown(text: string): JSX.Element[] {
    return [<Dropdown.Divider key={EMPTY_ID} id={EMPTY_ID} text={text} />]
  }
}

export default LogsHeader

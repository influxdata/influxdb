// Libraries
import React, {PureComponent} from 'react'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CollectorList from 'src/organizations/components/CollectorList'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  EmptyState,
} from 'src/clockface'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Telegraf} from 'src/api'
import {getTelegrafConfigTOML} from 'src/organizations/apis/index'
import {notify} from 'src/shared/actions/notifications'

interface Props {
  collectors: Telegraf[]
  onChange: () => void
  notify: NotificationsActions.PublishNotificationActionCreator
}

@ErrorHandling
export default class OrgOptions extends PureComponent<Props> {
  public render() {
    const {collectors} = this.props
    return (
      <>
        <TabbedPageHeader>
          <h1>Collectors</h1>
          <Button
            text="Create Collector"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
          />
        </TabbedPageHeader>
        <CollectorList
          collectors={collectors}
          emptyState={this.emptyState}
          onDownloadConfig={this.handleDownloadConfig}
        />
      </>
    )
  }
  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Collectors match your query" />
      </EmptyState>
    )
  }

  private handleDownloadConfig = async (telegrafID: string) => {
    try {
      const config = await getTelegrafConfigTOML(telegrafID)
      downloadTextFile(config, 'config.toml')
    } catch (error) {
      notify(getTelegrafConfigFailed())
    }
  }
}

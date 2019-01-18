// Libraries
import React, {PureComponent} from 'react'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import CollectorList from 'src/organizations/components/CollectorList'
import TelegrafExplainer from 'src/organizations/components/TelegrafExplainer'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  EmptyState,
  Grid,
  Columns,
} from 'src/clockface'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Telegraf} from 'src/api'
import {
  getTelegrafConfigTOML,
  deleteTelegrafConfig,
} from 'src/organizations/apis/index'
import {notify} from 'src/shared/actions/notifications'

interface Props {
  collectors: Telegraf[]
  onChange: () => void
  notify: NotificationsActions.PublishNotificationActionCreator
  orgName: string
}

@ErrorHandling
export default class Collectors extends PureComponent<Props> {
  public render() {
    const {collectors} = this.props
    return (
      <>
        <TabbedPageHeader>
          <h1>Telegraf Configurations</h1>
          <Button
            text="Create Configuration"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
          />
        </TabbedPageHeader>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={Columns.Twelve}>
              <CollectorList
                collectors={collectors}
                emptyState={this.emptyState}
                onDownloadConfig={this.handleDownloadConfig}
                onDelete={this.handleDeleteTelegraf}
              />
            </Grid.Column>
            <Grid.Column
              widthSM={Columns.Six}
              widthMD={Columns.Four}
              offsetSM={Columns.Three}
              offsetMD={Columns.Four}
            >
              <TelegrafExplainer />
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </>
    )
  }
  private get emptyState(): JSX.Element {
    const {orgName} = this.props

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text
          text={`${orgName} does not own any Telegraf  Configurations , why not create one?`}
          highlightWords={['Telegraf', 'Configurations']}
        />
        <Button
          text="Create Configuration"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
        />
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
  private handleDeleteTelegraf = async (telegrafID: string) => {
    await deleteTelegrafConfig(telegrafID)
    this.props.onChange()
  }
}

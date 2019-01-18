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
import DataLoadersWizard from 'src/dataLoaders/components/DataLoadersWizard'

// APIS
import {
  getTelegrafConfigTOML,
  deleteTelegrafConfig,
} from 'src/organizations/apis/index'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import {notify} from 'src/shared/actions/notifications'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Telegraf, Bucket} from 'src/api'
import {DataLoaderType, DataLoaderStep} from 'src/types/v2/dataLoaders'
import {OverlayState} from 'src/types/v2'

interface Props {
  collectors: Telegraf[]
  onChange: () => void
  notify: NotificationsActions.PublishNotificationActionCreator
  orgName: string
  buckets: Bucket[]
}

interface State {
  overlayState: OverlayState
}

@ErrorHandling
export default class Collectors extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {overlayState: OverlayState.Closed}
  }

  public render() {
    const {collectors, buckets} = this.props
    return (
      <>
        <TabbedPageHeader>
          <h1>Telegraf Configurations</h1>
          {this.createButton}
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
        <DataLoadersWizard
          visible={this.isOverlayVisible}
          onCompleteSetup={this.handleDismissDataLoaders}
          startingType={DataLoaderType.Streaming}
          startingStep={DataLoaderStep.Select}
          startingSubstep={'streaming'}
          buckets={buckets}
        />
      </>
    )
  }

  private get isOverlayVisible(): boolean {
    return this.state.overlayState === OverlayState.Open
  }

  private get createButton(): JSX.Element {
    return (
      <Button
        text="Create Configuration"
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        onClick={this.handleAddScraper}
      />
    )
  }

  private handleAddScraper = () => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleDismissDataLoaders = () => {
    this.setState({overlayState: OverlayState.Closed})
    this.props.onChange()
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text
          text={`${orgName} does not own any Telegraf  Configurations , why not create one?`}
          highlightWords={['Telegraf', 'Configurations']}
        />
        {this.createButton}
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

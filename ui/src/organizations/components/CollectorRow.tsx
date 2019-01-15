// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
  Button,
  ComponentColor,
  ComponentSpacer,
} from 'src/clockface'
import {Telegraf} from 'src/api'

interface Props {
  collector: Telegraf
  bucket: string
  onDownloadConfig: (telegrafID: string) => void
}

export default class BucketRow extends PureComponent<Props> {
  public render() {
    const {collector, bucket} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>{collector.name}</IndexList.Cell>
          <IndexList.Cell>{bucket}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ComponentSpacer align={Alignment.Right}>
              <Button
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Secondary}
                text={'Download Config'}
                onClick={this.handleDownloadConfig}
              />
              <ConfirmationButton
                size={ComponentSize.ExtraSmall}
                text="Delete"
                confirmText="Confirm"
              />
            </ComponentSpacer>
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleDownloadConfig = (): void => {
    this.props.onDownloadConfig(this.props.collector.id)
  }
}

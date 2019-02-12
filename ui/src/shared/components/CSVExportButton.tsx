// Libraries
import React, {PureComponent} from 'react'
import moment from 'moment'

// Components
import {Button, ComponentStatus, IconFont} from '@influxdata/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

interface Props {
  files: string[] | null
}

class CSVExportButton extends PureComponent<Props, {}> {
  public render() {
    return (
      <Button
        titleText={this.titleText}
        text="CSV"
        icon={IconFont.Download}
        onClick={this.handleClick}
        status={this.buttonStatus}
      />
    )
  }

  private get buttonStatus(): ComponentStatus {
    const {files} = this.props

    if (files) {
      return ComponentStatus.Default
    }

    return ComponentStatus.Disabled
  }

  private get titleText(): string {
    const {files} = this.props

    if (files) {
      return 'Download query results as a .CSV file'
    }

    return 'Create a query in order to download results as .CSV'
  }

  private handleClick = () => {
    const {files} = this.props
    const csv = files.join('\n\n')
    const now = moment().format('YYYY-MM-DD-HH-mm')
    const filename = `${now} Chronograf Data.csv`

    downloadTextFile(csv, filename, 'text/csv')
  }
}

export default CSVExportButton

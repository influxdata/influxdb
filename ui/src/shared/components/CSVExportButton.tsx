// Libraries
import React, {PureComponent} from 'react'
import moment from 'moment'

// Components
import {Button, ComponentStatus, IconFont} from 'src/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

interface Props {
  files: string[] | null
}

class CSVExportButton extends PureComponent<Props, {}> {
  public render() {
    const {files} = this.props

    return (
      <Button
        text="CSV"
        icon={IconFont.Download}
        onClick={this.handleClick}
        status={files ? ComponentStatus.Default : ComponentStatus.Disabled}
      />
    )
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

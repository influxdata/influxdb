import React, {PureComponent, ReactElement} from 'react'
import {Link} from 'react-router'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import ConnectionLink from 'src/sources/components/ConnectionLink'

import {Source} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  currentSource: Source
  onDeleteSource: (source: Source) => void
}

@ErrorHandling
class InfluxTableRow extends PureComponent<Props> {
  public render() {
    const {source, currentSource} = this.props

    return (
      <tr className={this.className}>
        <td>{this.connectButton}</td>
        <td>
          <ConnectionLink source={source} currentSource={currentSource} />
          <span>{source.url}</span>
        </td>
        <td className="text-right">
          <ConfirmButton
            type="btn-danger"
            size="btn-xs"
            text="Delete Connection"
            confirmAction={this.handleDeleteSource}
            customClass="delete-source table--show-on-row-hover"
          />
        </td>
      </tr>
    )
  }

  private handleDeleteSource = (): void => {
    this.props.onDeleteSource(this.props.source)
  }

  private get connectButton(): ReactElement<HTMLDivElement> {
    const {source} = this.props
    if (this.isCurrentSource) {
      return (
        <div className="btn btn-success btn-xs source-table--connect">
          Connected
        </div>
      )
    }

    return (
      <Link
        className="btn btn-default btn-xs source-table--connect"
        to={`/sources/${source.id}/manage-sources`}
      >
        Connect
      </Link>
    )
  }

  private get className(): string {
    if (this.isCurrentSource) {
      return 'highlight'
    }

    return ''
  }

  private get isCurrentSource(): boolean {
    const {source, currentSource} = this.props
    return source.id === currentSource.id
  }
}

export default InfluxTableRow

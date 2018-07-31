import React, {PureComponent, ReactElement} from 'react'
import {Link} from 'react-router'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import ConnectionLink from 'src/sources/components/ConnectionLink'
import FluxDropdown from 'src/sources/components/FluxDropdown'

import {Source, Service} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  currentSource: Source
  services: Service[]
  onDeleteSource: (source: Source) => void
  setActiveFlux: (source: Source, service: Service) => void
  deleteFlux: (fluxService: Service) => void
}

@ErrorHandling
class InfluxTableRow extends PureComponent<Props> {
  public render() {
    const {
      source,
      services,
      currentSource,
      setActiveFlux,
      deleteFlux,
    } = this.props

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
        to={`/sources/${source.id}/hosts`}
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

import React, {PureComponent} from 'react'
import DatabaseDropdown from 'src/shared/components/DatabaseDropdown'
import {Source, DropdownItem} from 'src/types'

interface Props {
  handleSelectDatabase: (item: DropdownItem) => void
  selectedDatabase: string
  toggleWriteView: (isWriteViewToggled: boolean) => void
  errorThrown: () => void
  onClose: () => void
  isManual: boolean
  source: Source
}

class WriteDataHeader extends PureComponent<Props> {
  public render() {
    const {
      handleSelectDatabase,
      selectedDatabase,
      errorThrown,
      onClose,
      source,
    } = this.props

    return (
      <div className="write-data-form--header">
        <div className="page-header__left">
          <h1 className="page-header__title">Write Data To</h1>
          <DatabaseDropdown
            source={source}
            onSelectDatabase={handleSelectDatabase}
            database={selectedDatabase}
            onErrorThrown={errorThrown}
          />
          <ul className="nav nav-tablist nav-tablist-sm">
            <li onClick={this.handleToggleOff} className={this.fileUploadClass}>
              File Upload
            </li>
            <li
              onClick={this.handleToggleOn}
              className={this.manualEntryClass}
              data-test="manual-entry-button"
            >
              Manual Entry
            </li>
          </ul>
        </div>
        <div className="page-header__right">
          <span className="page-header__dismiss" onClick={onClose} />
        </div>
      </div>
    )
  }

  private get fileUploadClass(): string {
    if (this.props.isManual) {
      return ''
    }

    return 'active'
  }

  private get manualEntryClass(): string {
    if (this.props.isManual) {
      return 'active'
    }

    return ''
  }

  private handleToggleOff = (): void => {
    this.props.toggleWriteView(false)
  }

  private handleToggleOn = (): void => {
    this.props.toggleWriteView(true)
  }
}

export default WriteDataHeader

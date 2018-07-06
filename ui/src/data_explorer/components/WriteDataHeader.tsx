import React, {PureComponent} from 'react'
import DatabaseDropdown from 'src/shared/components/DatabaseDropdown'
import RadioButtons from 'src/reusable_ui/components/radio_buttons/RadioButtons'
import {Source, DropdownItem} from 'src/types'
import {WriteDataMode} from 'src/types'

interface Props {
  handleSelectDatabase: (item: DropdownItem) => void
  selectedDatabase: string
  onToggleMode: (mode: WriteDataMode) => void
  errorThrown: () => void
  onClose: () => void
  mode: string
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
        <div className="page-header--left">
          <h1 className="page-header--title">Write Data To</h1>
          <DatabaseDropdown
            source={source}
            onSelectDatabase={handleSelectDatabase}
            database={selectedDatabase}
            onErrorThrown={errorThrown}
          />
          {this.modeSelector}
        </div>
        <div className="page-header--right">
          <span className="page-header__dismiss" onClick={onClose} />
        </div>
      </div>
    )
  }

  private get modeSelector(): JSX.Element {
    const {mode} = this.props
    const modes = [WriteDataMode.File, WriteDataMode.Manual]

    return (
      <RadioButtons
        buttons={modes}
        activeButton={mode}
        onChange={this.handleRadioButtonClick}
      />
    )
  }

  private handleRadioButtonClick = (mode: WriteDataMode): void => {
    const {onToggleMode} = this.props

    onToggleMode(mode)
  }
}

export default WriteDataHeader

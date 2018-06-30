import React, {PureComponent} from 'react'
import DatabaseDropdown from 'src/shared/components/DatabaseDropdown'
import RadioButtons, {
  RadioButton,
} from 'src/reusable_ui/components/radio_buttons/RadioButtons'
import {Source, DropdownItem} from 'src/types'

interface Props {
  handleSelectDatabase: (item: DropdownItem) => void
  selectedDatabase: string
  onToggleMode: (mode: RadioButton) => void
  errorThrown: () => void
  onClose: () => void
  mode: RadioButton
  modes: RadioButton[]
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
    const {mode, modes} = this.props

    return (
      <RadioButtons
        buttons={modes}
        activeButton={mode}
        onChange={this.handleRadioButtonClick}
      />
    )
  }

  private handleRadioButtonClick = (button: RadioButton): void => {
    const {onToggleMode} = this.props

    onToggleMode(button)
  }
}

export default WriteDataHeader

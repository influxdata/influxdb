// Libraries
import React, {Component, ChangeEvent, CSSProperties} from 'react'

// Components
import {
  Input,
  Dropdown,
  ComponentStatus,
  ComponentSize,
  DropdownMenuTheme,
  ComponentColor,
} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  testID: string
  className?: string
  searchTerm?: string
  searchPlaceholder?: string
  selectedOption: string
  onSelect: (option: string) => void
  onChangeSearchTerm?: (value: string) => void
  buttonSize: ComponentSize
  buttonColor: ComponentColor
  buttonStatus: ComponentStatus
  buttonTestID: string
  menuTheme: DropdownMenuTheme
  menuTestID: string
  options: (string | number)[]
  emptyText: string
  style?: CSSProperties
}

@ErrorHandling
export default class SearchableDropdown extends Component<Props> {
  public static defaultProps = {
    buttonSize: ComponentSize.Small,
    buttonColor: ComponentColor.Default,
    buttonStatus: ComponentStatus.Default,
    menuTheme: DropdownMenuTheme.Onyx,
    testID: 'searchable-dropdown',
    buttonTestID: 'searchable-dropdown--button',
    menuTestID: 'searchable-dropdown--menu',
  }

  public render() {
    const {
      searchTerm,
      searchPlaceholder,
      buttonSize,
      buttonColor,
      buttonStatus,
      buttonTestID,
      selectedOption,
      testID,
      className,
      style,
      menuTheme,
      menuTestID,
    } = this.props

    return (
      <Dropdown
        testID={testID}
        className={className}
        style={style}
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            testID={buttonTestID}
            color={buttonColor}
            size={buttonSize}
            status={buttonStatus}
          >
            {buttonStatus === ComponentStatus.Loading
              ? 'Loading...'
              : selectedOption}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu
            onCollapse={onCollapse}
            theme={menuTheme}
            testID={menuTestID}
          >
            <div className="searchable-dropdown--input-container">
              <Input
                onChange={this.handleChange}
                value={searchTerm}
                placeholder={searchPlaceholder}
                size={buttonSize}
                autoFocus={true}
              />
            </div>
            {this.filteredMenuOptions}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private get filteredMenuOptions(): JSX.Element[] | JSX.Element {
    const {
      searchTerm,
      options,
      emptyText,
      selectedOption,
      onSelect,
    } = this.props

    const filteredOptions = options.filter(option =>
      `${option}`.toLocaleLowerCase().includes(searchTerm.toLocaleLowerCase())
    )

    if (!filteredOptions.length) {
      return <div className="searchable-dropdown--empty">{emptyText}</div>
    }

    return filteredOptions.map(option => (
      <Dropdown.Item
        key={option}
        value={option}
        selected={option === selectedOption}
        onClick={onSelect}
        testID={`searchable-dropdown--item ${option}`}
      >
        {option}
      </Dropdown.Item>
    ))
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onChangeSearchTerm} = this.props

    if (onChangeSearchTerm) {
      onChangeSearchTerm(e.target.value)
    }
  }
}

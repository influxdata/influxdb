import React, {
  PureComponent,
  FocusEvent,
  ChangeEvent,
  KeyboardEvent,
} from 'react'
import Dropdown from 'src/shared/components/Dropdown'

import {NULL_STRING, NUMBER} from 'src/shared/constants/queryFillOptions'

import queryFills from 'src/shared/data/queryFills'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onChooseFill: (text: string) => void
  value: string
  size?: string
  theme?: string
  isDisabled?: boolean
}

interface Item {
  type: string
  text: string
}
interface State {
  selected: Item
  currentNumberValue: string
  resetNumberValue: string
}

@ErrorHandling
class FillQuery extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    size: 'sm',
    theme: 'blue',
    value: NULL_STRING,
  }

  private numberInput: HTMLElement

  constructor(props) {
    super(props)

    const isNumberValue: boolean = !isNaN(Number(props.value))

    this.state = isNumberValue
      ? {
          selected: queryFills.find(fill => fill.type === NUMBER),
          currentNumberValue: props.value,
          resetNumberValue: props.value,
        }
      : {
          selected: queryFills.find(fill => fill.type === props.value),
          currentNumberValue: '0',
          resetNumberValue: '0',
        }
  }

  public render() {
    const {size, theme, isDisabled} = this.props
    const {selected, currentNumberValue} = this.state

    return (
      <div className={`fill-query fill-query--${size}`}>
        {selected.type === NUMBER && (
          <input
            ref={r => (this.numberInput = r)}
            type="number"
            className={`form-control monotype form-${this.getColor(
              theme
            )} input-${size} fill-query--input`}
            placeholder="Custom Value"
            value={currentNumberValue}
            onKeyUp={this.handleKeyUp}
            onKeyDown={this.handleKeyDown}
            onChange={this.handleInputChange}
            onBlur={this.handleInputBlur}
          />
        )}
        <Dropdown
          selected={selected.text}
          items={queryFills}
          className="fill-query--dropdown dropdown-100"
          buttonSize={`btn-${size}`}
          buttonColor="btn-info"
          menuClass={`dropdown-${this.getColor(theme)}`}
          onChoose={this.handleDropdown}
          disabled={isDisabled}
        />
        <label className="fill-query--label">Fill:</label>
      </div>
    )
  }

  private handleDropdown = (item: Item): void => {
    if (item.text === NUMBER) {
      this.setState({selected: item}, () => {
        this.numberInput.focus()
      })
    } else {
      this.setState({selected: item}, () => {
        this.props.onChooseFill(item.text)
      })
    }
  }

  private handleInputBlur = (e: FocusEvent<HTMLInputElement>): void => {
    const nextNumberValue = e.target.value
      ? e.target.value
      : this.state.resetNumberValue || '0'

    this.setState({
      currentNumberValue: nextNumberValue,
      resetNumberValue: nextNumberValue,
    })

    this.props.onChooseFill(nextNumberValue)
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const currentNumberValue = e.target.value

    this.setState({currentNumberValue})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      this.numberInput.blur()
    }
  }

  private handleKeyUp = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Escape') {
      this.setState({currentNumberValue: this.state.resetNumberValue}, () => {
        this.numberInput.blur()
      })
    }
  }

  private getColor = (theme: string): string => {
    switch (theme) {
      case 'BLUE':
        return 'plutonium'
      case 'GREEN':
        return 'malachite'
      case 'PURPLE':
        return 'astronaut'
      default:
        return 'plutonium'
    }
  }
}

export default FillQuery

import React, {Component} from 'react'
import classnames from 'classnames'
import moment from 'moment'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import timeRanges from 'src/logs/data/timeRanges'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import CustomTimeRange from 'src/shared/components/CustomTimeRange'

import {TimeRange} from 'src/types'

const dateFormat = 'YYYY-MM-DD HH:mm'
const emptyTime = {lower: '', upper: ''}
const format = t => moment(t.replace(/\'/g, '')).format(dateFormat)

interface Props {
  selected: {
    lower: string
    upper?: string
  }

  onChooseTimeRange: (timeRange: TimeRange) => void
  preventCustomTimeRange?: boolean
  page?: string
}

interface State {
  autobind: boolean
  isOpen: boolean
  isCustomTimeRangeOpen: boolean
  customTimeRange: TimeRange
}

@ErrorHandling
class TimeRangeDropdown extends Component<Props, State> {
  public static defaultProps = {
    page: 'default',
  }

  constructor(props) {
    super(props)
    const {lower, upper} = props.selected

    const isTimeValid = moment(upper).isValid() && moment(lower).isValid()
    const customTimeRange = isTimeValid ? {lower, upper} : emptyTime

    this.state = {
      autobind: false,
      isOpen: false,
      isCustomTimeRangeOpen: false,
      customTimeRange,
    }
  }

  public render() {
    const {selected, preventCustomTimeRange, page} = this.props
    const {customTimeRange, isCustomTimeRangeOpen} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className="time-range-dropdown">
          <div className={this.dropdownClassName}>
            <div
              className="btn btn-sm btn-default dropdown-toggle"
              onClick={this.toggleMenu}
            >
              <span className="icon clock" />
              <span className="dropdown-selected">
                {this.findTimeRangeInputValue(selected)}
              </span>
              <span className="caret" />
            </div>
            <ul className="dropdown-menu">
              <FancyScrollbar
                autoHide={false}
                autoHeight={true}
                maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
              >
                {preventCustomTimeRange ? null : (
                  <div>
                    <li className="dropdown-header">Absolute Time</li>
                    <li
                      className={
                        isCustomTimeRangeOpen
                          ? 'active dropdown-item custom-timerange'
                          : 'dropdown-item custom-timerange'
                      }
                    >
                      <a href="#" onClick={this.showCustomTimeRange}>
                        Date Picker
                      </a>
                    </li>
                  </div>
                )}
                <li className="dropdown-header">
                  {preventCustomTimeRange ? '' : 'Relative '}Time
                </li>
                {timeRanges.map(item => {
                  return (
                    <li className="dropdown-item" key={item.menuOption}>
                      <a href="#" onClick={this.handleSelection(item)}>
                        {item.menuOption}
                      </a>
                    </li>
                  )
                })}
              </FancyScrollbar>
            </ul>
          </div>
          {isCustomTimeRangeOpen ? (
            <ClickOutside onClickOutside={this.handleCloseCustomTimeRange}>
              <div className="custom-time--overlay">
                <CustomTimeRange
                  onApplyTimeRange={this.handleApplyCustomTimeRange}
                  timeRange={customTimeRange}
                  onClose={this.handleCloseCustomTimeRange}
                  isVisible={isCustomTimeRangeOpen}
                  timeInterval={60}
                  page={page}
                />
              </div>
            </ClickOutside>
          ) : null}
        </div>
      </ClickOutside>
    )
  }

  private get dropdownClassName(): string {
    const {
      isOpen,
      customTimeRange: {lower, upper},
    } = this.state

    const absoluteTimeRange = !!lower || !!upper

    return classnames('dropdown', {
      'dropdown-290': absoluteTimeRange,
      'dropdown-120': !absoluteTimeRange,
      open: isOpen,
    })
  }

  private findTimeRangeInputValue = ({upper, lower}: TimeRange) => {
    if (upper && lower) {
      if (upper === 'now()') {
        return `${format(lower)} - Now`
      }

      return `${format(lower)} - ${format(upper)}`
    }

    const selected = timeRanges.find(range => range.lower === lower)
    return selected ? selected.inputValue : 'Custom'
  }

  private handleClickOutside = () => {
    this.setState({isOpen: false})
  }

  private handleSelection = timeRange => () => {
    this.props.onChooseTimeRange(timeRange)
    this.setState({customTimeRange: emptyTime, isOpen: false})
  }

  private toggleMenu = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  private showCustomTimeRange = () => {
    this.setState({isCustomTimeRangeOpen: true})
  }

  private handleApplyCustomTimeRange = customTimeRange => {
    this.props.onChooseTimeRange({...customTimeRange})
    this.setState({customTimeRange, isOpen: false})
  }

  private handleCloseCustomTimeRange = () => {
    this.setState({isCustomTimeRangeOpen: false})
  }
}
export default TimeRangeDropdown

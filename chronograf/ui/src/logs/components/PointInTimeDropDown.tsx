import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'
import moment from 'moment'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import timePoints from 'src/logs/data/timePoints'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import CustomSingularTime from 'src/shared/components/CustomSingularTime'

interface Props {
  customTime?: string
  relativeTime?: number
  onChooseCustomTime: (time: string) => void
  onChooseRelativeTime: (time: number) => void
}

interface State {
  isOpen: boolean
  isTimeSelectorOpen: boolean
}

const dateFormat = 'YYYY-MM-DD HH:mm'
const format = (t: string) => moment(t.replace(/\'/g, '')).format(dateFormat)

@ErrorHandling
class TimeRangeDropdown extends Component<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      isTimeSelectorOpen: false,
    }
  }

  public render() {
    const {isTimeSelectorOpen} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div
          className="time-range-dropdown logs-viewer--search-dropdown"
          style={{display: 'inline'}}
        >
          <div className={this.dropdownClassName}>
            <div
              className="btn btn-sm btn-default dropdown-toggle"
              onClick={this.toggleMenu}
            >
              <span className="icon clock" />
              <span className="dropdown-selected">{this.timeInputValue}</span>
              <span className="caret" />
            </div>
            <ul className="dropdown-menu">
              <FancyScrollbar
                autoHide={false}
                autoHeight={true}
                maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
              >
                <div>
                  <li className="dropdown-header">Absolute Time</li>
                  <li
                    className={
                      isTimeSelectorOpen
                        ? 'active dropdown-item custom-timerange'
                        : 'dropdown-item custom-timerange'
                    }
                  >
                    <a href="#" onClick={this.handleOpenCustomTime}>
                      Date Picker
                    </a>
                  </li>
                </div>
                <li className="dropdown-header">Relative Time</li>
                {timePoints.map(point => {
                  return (
                    <li className="dropdown-item" key={`pot-${point.value}`}>
                      <a
                        href="#"
                        onClick={this.handleSelection}
                        data-value={point.value}
                      >
                        {point.text}
                      </a>
                    </li>
                  )
                })}
              </FancyScrollbar>
            </ul>
          </div>
          {isTimeSelectorOpen ? (
            <ClickOutside onClickOutside={this.handleCloseCustomTime}>
              <div className="custom-time--overlay">
                <CustomSingularTime
                  onSelected={this.handleCustomSelection}
                  time={this.props.customTime}
                />
              </div>
            </ClickOutside>
          ) : null}
        </div>
      </ClickOutside>
    )
  }

  private get dropdownClassName(): string {
    const {isOpen} = this.state
    const absoluteTimeRange = !!this.props.customTime

    return classnames('dropdown', {
      'dropdown-180': absoluteTimeRange,
      'dropdown-110': !absoluteTimeRange,
      open: isOpen,
    })
  }

  private handleCustomSelection = (time: string) => {
    this.handleCloseCustomTime()
    this.props.onChooseCustomTime(time)
    this.setState({isOpen: false})
  }

  private handleSelection = (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    const {dataset} = e.target as HTMLAnchorElement
    this.props.onChooseRelativeTime(+dataset.value)
    this.setState({isOpen: false})
  }

  private get timeInputValue(): string {
    if (!this.props.customTime) {
      const point = timePoints.find(p => p.value === this.props.relativeTime)
      if (point) {
        return point.text
      }

      return 'now'
    }

    return format(this.props.customTime)
  }

  private handleClickOutside = () => {
    this.setState({isOpen: false})
  }

  private toggleMenu = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  private handleCloseCustomTime = () => {
    this.setState({isTimeSelectorOpen: false})
  }

  private handleOpenCustomTime = () => {
    this.setState({isTimeSelectorOpen: true})
  }
}
export default TimeRangeDropdown

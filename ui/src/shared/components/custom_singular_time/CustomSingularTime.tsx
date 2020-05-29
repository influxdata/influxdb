import React, {Component} from 'react'
import rome from 'rome'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {formatTimeRange} from 'src/shared/utils/time'

interface Props {
  onSelected: (time: string) => void
  time: string
  timeInterval?: number
  onClose?: () => void
}

interface State {
  time: string
}

@ErrorHandling
class CustomSingularTime extends Component<Props, State> {
  private calendar?: any
  private containerRef: React.RefObject<HTMLDivElement> = React.createRef<
    HTMLDivElement
  >()
  private inputRef: React.RefObject<HTMLInputElement> = React.createRef<
    HTMLInputElement
  >()

  constructor(props: Props) {
    super(props)

    this.state = {
      time: props.time,
    }
  }

  public componentDidMount() {
    const {time, timeInterval} = this.props

    this.calendar = rome(this.inputRef.current, {
      appendTo: this.containerRef.current,
      initialValue: formatTimeRange(time),
      autoClose: false,
      autoHideOnBlur: false,
      autoHideOnClick: false,
      timeInterval,
    })

    this.calendar.show()
  }

  public render() {
    return (
      <div className="custom-time-container">
        <div className="custom-time--wrap">
          <div className="custom-time--dates">
            <div
              className="custom-time--lower-container"
              ref={this.containerRef}
            >
              <input
                ref={this.inputRef}
                className="custom-time--lower form-control input-sm"
                onKeyUp={this.handleRefreshCalendar}
              />
            </div>
          </div>
          <div
            style={{marginTop: '10px'}}
            className="custom-time--apply btn btn-sm btn-primary"
            onClick={this.handleClick}
          >
            Apply
          </div>
        </div>
      </div>
    )
  }

  private handleRefreshCalendar = () => {
    if (this.calendar) {
      this.calendar.refresh()
    }
  }

  private handleClick = () => {
    const date = this.calendar.getDate()
    if (date) {
      const time = date.toISOString()
      this.props.onSelected(time)
    }

    if (this.props.onClose) {
      this.props.onClose()
    }
  }
}

export default CustomSingularTime

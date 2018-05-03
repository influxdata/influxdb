import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  selected: string
  measurement: string
  onChooseTag: () => void
  onChooseMeasurement: (measurement: string) => void
}

interface State {
  isOpen: boolean
}

@ErrorHandling
class MeasurementListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {isOpen: this.isCurrentMeasurement}
  }

  public render() {
    const {measurement} = this.props

    return (
      <div key={measurement} onClick={this.handleClick}>
        <div className="query-builder--list-item">
          <span>
            <div className="query-builder--caret icon caret-right" />
            {measurement}
          </span>
        </div>
      </div>
    )
  }

  private handleClick = () => {
    const {measurement, onChooseMeasurement} = this.props

    if (!this.isCurrentMeasurement) {
      this.setState({isOpen: true}, () => {
        onChooseMeasurement(measurement)
      })
    } else {
      this.setState({isOpen: !this.state.isOpen})
    }
  }

  private get isCurrentMeasurement(): boolean {
    const {selected, measurement} = this.props
    return selected === measurement
  }
}

export default MeasurementListItem

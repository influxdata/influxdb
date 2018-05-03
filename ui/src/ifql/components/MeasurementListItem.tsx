import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import TagList from 'src/ifql/components/TagList'

interface Props {
  db: string
  selected: string
  measurement: string
  onChooseMeasurement: (measurement: string) => void
}

interface State {
  isOpen: boolean
}

@ErrorHandling
class MeasurementListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {isOpen: false}
  }

  public render() {
    const {measurement, db} = this.props

    return (
      <div key={measurement} onClick={this.handleClick}>
        <div className="query-builder--list-item">
          <span>
            <div className="query-builder--caret icon caret-right" />
            {measurement}
          </span>
        </div>
        {this.shouldShow && <TagList db={db} measurement={measurement} />}
      </div>
    )
  }

  private handleClick = () => {
    const {measurement, onChooseMeasurement} = this.props
    onChooseMeasurement(measurement)
  }

  private get shouldShow(): boolean {
    return this.state.isOpen
  }
}

export default MeasurementListItem

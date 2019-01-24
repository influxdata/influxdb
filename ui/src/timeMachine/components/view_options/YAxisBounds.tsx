// Libraries
import React, {PureComponent} from 'react'

// Components
import YAxisBound from 'src/timeMachine/components/view_options/YAxisBound'

interface Props {
  min: string
  max: string
  scale: string
  onUpdateYAxisMinBound: (min: string) => void
  onUpdateYAxisMaxBound: (max: string) => void
}

class YAxisBounds extends PureComponent<Props> {
  public render() {
    const {
      min,
      max,
      scale,
      onUpdateYAxisMinBound,
      onUpdateYAxisMaxBound,
    } = this.props

    return (
      <>
        <YAxisBound
          label="Min"
          bound={min}
          scale={scale}
          onUpdateYAxisBound={onUpdateYAxisMinBound}
        />

        <YAxisBound
          label="Max"
          bound={max}
          scale={scale}
          onUpdateYAxisBound={onUpdateYAxisMaxBound}
        />
      </>
    )
  }
}

export default YAxisBounds

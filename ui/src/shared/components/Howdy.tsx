import React, {Component} from 'react'
import AutoRefresh from 'src/shared/components/AutoRefresh/AutoRefresh'
import TimeSeries from 'src/shared/components/TimeSeries/TimeSeries'

class Howdy extends Component {
  public render() {
    return (
      <AutoRefresh autoRefresh={0}>
        <TimeSeries />
      </AutoRefresh>
    )
  }
}

export default Howdy

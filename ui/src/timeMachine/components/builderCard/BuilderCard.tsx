// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import BuilderCardHeader from 'src/timeMachine/components/builderCard/BuilderCardHeader'
import BuilderCardMenu from 'src/timeMachine/components/builderCard/BuilderCardMenu'
import BuilderCardBody from 'src/timeMachine/components/builderCard/BuilderCardBody'
import BuilderCardEmpty from 'src/timeMachine/components/builderCard/BuilderCardEmpty'

interface Props {
  testID: string
  className?: string
}

export default class BuilderCard extends PureComponent<Props> {
  public static Header = BuilderCardHeader
  public static Menu = BuilderCardMenu
  public static Body = BuilderCardBody
  public static Empty = BuilderCardEmpty

  public static defaultProps = {
    testID: 'builder-card',
  }

  public render() {
    const {children, testID, className} = this.props

    const classname = classnames('builder-card', {[`${className}`]: className})

    return (
      <div className={classname} data-testid={testID}>
        {children}
      </div>
    )
  }
}

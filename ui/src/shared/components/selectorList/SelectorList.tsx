// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import SelectorListHeader from 'src/shared/components/selectorList/SelectorListHeader'
import SelectorListMenu from 'src/shared/components/selectorList/SelectorListMenu'
import SelectorListBody from 'src/shared/components/selectorList/SelectorListBody'
import SelectorListEmpty from 'src/shared/components/selectorList/SelectorListEmpty'

interface Props {
  testID: string
  className?: string
}

export default class SelectorList extends PureComponent<Props> {
  public static Header = SelectorListHeader
  public static Menu = SelectorListMenu
  public static Body = SelectorListBody
  public static Empty = SelectorListEmpty

  public static defaultProps = {
    testID: 'selector-list',
  }

  public render() {
    const {children, testID, className} = this.props

    const classname = classnames('selector-list', {[`${className}`]: className})

    return (
      <div className={classname} data-testid={testID}>
        {children}
      </div>
    )
  }
}

// Libraries
import React, {Component} from 'react'

// Components
import TabContents from 'src/clockface/components/tabs/TabContents'
import TabsNav from 'src/clockface/components/tabs/TabsNav'
import NavigationTab from 'src/clockface/components/tabs/NavigationTab'
import TabContentsHeader from 'src/clockface/components/tabs/TabContentsHeader'

class Tabs extends Component {
  public static TabContents = TabContents
  public static Nav = TabsNav
  public static Tab = NavigationTab
  public static TabContentsHeader = TabContentsHeader

  public render() {
    const {children} = this.props

    return <div className="tabs">{children}</div>
  }
}

export default Tabs

// Libraries
import React, {PureComponent} from 'react'

// Components
import ResourceListHeader from 'src/clockface/components/resource_list/ResourceListHeader'
import ResourceListSorter from 'src/clockface/components/resource_list/ResourceListSorter'
import ResourceListBody from 'src/clockface/components/resource_list/ResourceListBody'
import ResourceCard from 'src/clockface/components/resource_list/ResourceCard'
import ResourceName from 'src/clockface/components/resource_list/ResourceName'
import ResourceDescription from 'src/clockface/components/resource_list/ResourceDescription'

// Styles
import 'src/clockface/components/resource_list/ResourceList.scss'

interface Props {
  children: JSX.Element[] | JSX.Element
}

export default class ResourceList extends PureComponent<Props> {
  public static Header = ResourceListHeader
  public static Sorter = ResourceListSorter
  public static Body = ResourceListBody
  public static Card = ResourceCard
  public static Name = ResourceName
  public static Description = ResourceDescription

  public render() {
    return <div className="resource-list">{this.props.children}</div>
  }
}

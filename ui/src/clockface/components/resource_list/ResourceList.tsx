// Libraries
import React, {PureComponent} from 'react'

// Components
import ResourceListHeader from 'src/clockface/components/resource_list/ResourceListHeader'
import ResourceListSorter from 'src/clockface/components/resource_list/ResourceListSorter'
import ResourceListBody from 'src/clockface/components/resource_list/ResourceListBody'
import ResourceCard from 'src/clockface/components/resource_list/ResourceCard'
import ResourceEditableName from 'src/clockface/components/resource_list/ResourceEditableName'
import ResourceDescription from 'src/clockface/components/resource_list/ResourceDescription'
import ResourceName from 'src/clockface/components/resource_list/ResourceName'

interface Props {
  children: JSX.Element[] | JSX.Element
}

export default class ResourceList extends PureComponent<Props> {
  public static Header = ResourceListHeader
  public static Sorter = ResourceListSorter
  public static Body = ResourceListBody
  public static Card = ResourceCard
  public static Name = ResourceName
  public static EditableName = ResourceEditableName
  public static Description = ResourceDescription

  public render() {
    return <div className="resource-list">{this.props.children}</div>
  }
}

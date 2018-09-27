# Index View Components

These components are purely presentational and intended for use in index view pages (such as Dashboards index)
There are two types of index views:

1. Index List
2. Index Grid (Coming soon)

## How to Use Index List

`IndexList` is essentially a configurable table.

Import the component and associated types:

```
import {Alignment} from 'src/clockface'
import {
  IndexListColumn,
  IndexListRow,
} from 'src/shared/components/index_views/IndexListTypes'

import IndexList from 'src/shared/components/index_views/IndexList'
```

#### Define Your Columns

First define the columns present in your table. Each column has a few options for customization:

| Option      | Type      |                                                                                                                                       |
|-------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------|
| key         | string    | Unique identifier                                                                                                                     |
| title       | string    | Text that appears in column header                                                                                                    |
| size        | number    | Use as a proportion not a unit of measurement. `IndexList` totals the sizes of all columns and calculates each column as a percentage |
| showOnHover | boolean   | if `true` the column's contents will be hidden until a row is hovered                                                                 |
| align       | Alignment | Controls text alignment `Left | Center | Right`                                                                                       |

Example columns:

```
const columns = [
  {
    key: 'users--name',
    title: 'Name',
    size: 500,
    showOnHover: false,
    align: Alignment.Left,
  },
  {
    key: 'users--email',
    title: 'Email',
    size: 100,
    showOnHover: false,
    align: Alignment.Left,
  },
  {
    key: 'users--last-login',
    title: 'Last Login',
    size: 90,
    showOnHover: false,
    align: Alignment.Left,
  },
  {
    key: 'users--actions',
    title: '',
    size: 200,
    showOnHover: true,
    align: Alignment.Right,
  },
]
```

#### Define Your Rows

Next step is to define your rows. It is crucial that the `key` of each column has a matching `key` within each row or else the component won't render properly.

The `contents` field accepts any type of value. Setting `disabled` to true is just a visual style -- any interactive elements in that row should be individually disabled as well.

```
const rows = users.map(user => ({
  disabled: false,
  columns: [
    {
      key: 'users--name',
      contents: <p>{user.name}</p>,
    },
    {
      key: 'users--email',
      contents: <p>{user.email}</p>,
    },
    {
      key: 'users--login',
      contents: this.userLastLogin(user),
    },
    {
      key: 'users--actions',
      contents: (
        <ComponentSpacer align={Alignment.Left}>
          <Button>Edit User</Button>
          <Button>Remove from Org</Button>
        </ComponentSpacer>
      ),
    },
  ],
}))
```

#### Define Your Empty State

`IndexList` can be passed a `JSX.Element` that will appear when the list of `rows` is empty. This can be utilized a few different ways. For example, if you are allowing users to sort using a filter then you would pass the filtered list of rows into `IndexList`. The empty state you pass in would then depend on the presence of a `searchTerm` state to show either `No users exist` or `No users match your query`.

#### Render

Now that everything is defined and ready:

```
<IndexList columns={columns} rows={rows} emptyState={this.emptyList}>
```


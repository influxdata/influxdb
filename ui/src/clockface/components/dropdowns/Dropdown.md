# Dropdown

Dropdowns are a great way to offer users a lot of options in a nested, space-efficient manner. Clockface dropdowns have two distinct modes: `ActionList` and `Radio`. By default Dropdowns are in `Radio` mode as that is the more common use case.

`ActionList` does not have a concept of state and is essentially an expandable list of buttons. If you are using `ActionList` mode you must also pass in a `titleText` prop.

`Radio` assumes that one item in the dropdown list is the "selected" item at all times. If you are using `Radio` mode you must also pass in a `selectedID` prop. The dropdown keeps track of items by there `id` prop so that two items with the same name are still unique.

## How to Use

Dropdowns are constructed using a composable API. There are only 2 accepted child type:

- `<Dropdown.Item>`
- `<Dropdown.Divider>`

First, define your dropdown menu as an array of objects:

```
interface Color {
  label: string
  id: string
  hex: string
}

dropdownColors: Color[] = [
  {label: 'red', id: 'color-red', hex: '#ff0000'},
  {label: 'green', id: 'color-green', hex: '#00ff00'},
  {label: 'blue', id: 'color-blue', hex: '#0000ff'},
]
```

Then map over the array to pass in the children. I recommend having a key on each object called `key` or `id`.

```
<Dropdown
  selectedID={dropdownColors[0].id}
  onChange={this.handleDropdownChange}
>
  {dropdownColors.map(color => (
    <Dropdown.Item key={color.id} id={color.id} value={color}>
      {colorlabel}
    </Dropdown.Item>
  )}
</Dropdown>
```

`Dropdown.Item` returns the `value` prop when clicked, so I recommend passing the entire object into that prop. The `id` prop is used find and highlight the active item in the list.

```
private handleDropdownChange = (item: MenuItem): void => {
  // Do something with the returned item
  this.setState({selectedItem: item.id})
}
```

## Options

| Option        | Type                   | Default Value | Note                                                                                                                     |
|---------------|------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------|
| widthPixels   | number                 | `100%`        | Width of the dropdown in pixels                                                                                          |
| icon          | IconFont               |               | Render an icon on the left side of the toggle button                                                                     |
| wrapText      | boolean                | false         | If `true` the contents of each dropdown item will not exceed the width of the dropdown toggle button (aka `widthPixels`) |
| maxMenuHeight | number                 | 250           | Pixel height after which the dropdown menu will scroll                                                                   |
| customClass   | string                 |               | Useful when you want to apply custom positioning to the dropdown or override the appearance                              |
| status        | ComponentStatus        | Default       | Used to change the state of the component. Currently only accepts `Default` and `Disabled`                               |
| menuColor     | DropdownMenuColors     | Sapphire      | Changes the coloration of the dropdown menu independent of the toggle button                                             |
| buttonColor   | ComponentColor         | Default       | Changes the coloration of the dropdown toggle button                                                                     |
| buttonSize    | ComponentSize          | Small         | Changes the size of the dropdown toggle button                                                                           |
| mode          | DropdownMode           | Radio         | Changes the modality of the dropdown. `Radio` requires a `selectedID` also be specified                                  |
| titleText     | string                 |               | If the dropdown's mode is `ActionList` then `titleText` is used as the label in the dropdown toggle                      |
| selectedID    | string                 |               | If the dropdown's mode is `Radio` then `selectedID` is required to track the currently selected item                     |
| onChange      | `(value: any) => void` |               | When a dropdown item is click, its `value` prop is returned via `onChange`                                               |


## Adding Dividers

If you wish to separate out items in a dropdown it's fairly easy:

```
<Dropdown.Divider key={key} text="Name of section" />
```

The `text` prop is optional and if unspecified the divider appears as a thin line

I recommend defining dividers in your array of menu items and differentiate using a `type` field:

```
[
  {
    type: 'divider',
    key: 'divider-fruits`,
    name: 'Fruits',
  },
  {
    type: 'item',
    key: 'fruit-apple`,
    name: 'Apple',
  },
  {
    type: 'item',
    key: 'fruit-pear`,
    name: 'Pear',
  },
  {
    type: 'item',
    key: 'fruit-banana`,
    name: 'Banana',
  },
  {
    type: 'divider',
    key: 'divider-vegetables`,
    name: 'Vegetables',
  },
  {
    type: 'item',
    key: 'vegetable-carrot`,
    name: 'Carrot',
  },
  {
    type: 'item',
    key: 'vegetable-celery`,
    name: 'Celery',
  },
  {
    type: 'item',
    key: 'vegetable-onion`,
    name: 'Onion',
  },
]
```

When mapping over the menu items use the `type` field to decide whether to render a `<Dropdown.Item>` or `<Dropdown.Divider>`


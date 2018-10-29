# Clockface UI Kit

Clockface is a set of reusable presentational components intended for use with Chronograf or other Influx projects.
All components are exported from `src/clockface/index.ts` so only a single import is needed for usage.

### Component Documentation

- [Dropdown](components/dropdowns/Dropdown.md)
- Multi-Select Dropdown
- Button
- Radio Buttons
- Form Layout
- Input
- Slide Toggle
- Overlay
- Panel
- Card Select
- Wizard
- Component Spacer

### Enums

Most of the Clockface components have props expecting enums which are shared across the UI kit

- `ComponentStatus`
  - Used to change state of some components
  - `Default`
  - `Loading`
  - `Error`
  - `Valid`
  - `Disabled`
- `ComponentSize`
  - Used to control the size of some components
  - `ExtraSmall`
  - `Small`
  - `Medium`
  - `Large`
- `ComponentColor`
  - Used to control the coloration (or modality) of components
  - `Default` (Grey)
  - `Primary` (Blue)
  - `Secondary` (Purple)
  - `Success` (Green)
  - `Alert` (Yellow)
  - `Danger` (Red)
- `ButtonShape`
  - Used to control how certain components fill space
  - `Default`
  - `Square`
  - `StretchToFit`
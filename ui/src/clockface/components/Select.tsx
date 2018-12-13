import React, {SFC} from 'react'

interface WrapperProps<T> {
  type: JSX.Element['type']
  children: T[] | T
}

interface Options {
  count?: number
  strict?: boolean
}

type WrapSelection<T> = SFC<WrapperProps<T> & Options>

const Select: WrapSelection<JSX.Element> = ({
  type,
  children,
  strict = false,
  count = Infinity,
}): JSX.Element => (
  <>
    {React.Children.map(children, (child: JSX.Element) => {
      if (child.type === type && count-- > 0) {
        return child
      } else if (strict && child.type !== type) {
        throw new Error(`Expected ${type} but received ${child.type}`)
      }
    })}
  </>
)

export default Select

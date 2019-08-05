import {useState} from 'react'
import {useMountedEffect} from 'src/shared/utils/useMountedEffect'

export const useDebouncedValue = <T extends any>(
  value: T,
  delay: number
): T => {
  const [debouncedValue, setDebouncedValue] = useState(value)

  useMountedEffect(() => {
    const handler = setTimeout(() => setDebouncedValue(value), delay)

    return () => clearTimeout(handler)
  }, [value, delay])

  return debouncedValue
}

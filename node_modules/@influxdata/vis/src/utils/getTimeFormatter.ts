export const getTimeFormatter = (domain: number[]) => {
  const dateFormatter = getDateTickFormatter(domain)

  return x => dateFormatter(new Date(x))
}

const getDateTickFormatter = (domain: number[]) => {
  const domainWidth = domain[1] - domain[0]

  if (domainWidth < FORMATTERS[0][0]) {
    return FORMATTERS[0][1]
  }

  if (domainWidth > FORMATTERS[FORMATTERS.length - 1][0]) {
    return FORMATTERS[FORMATTERS.length - 1][1]
  }

  for (let i = 0; i < FORMATTERS.length - 1; i++) {
    if (domainWidth > FORMATTERS[i][0] && domainWidth <= FORMATTERS[i + 1][0]) {
      return FORMATTERS[i + 1][1]
    }
  }

  return x => String(x)
}

const leftPad = (x: number, n = 2, c = '0'): string => {
  const s = String(x)

  if (s.length >= n) {
    return s
  }

  return c.repeat(n - s.length) + s
}

// prettier-ignore
const FORMATTERS: [number, ((d: Date) => string)][] = [
  [
    1000,
    d => `${leftPad(d.getSeconds())}.${leftPad(d.getMilliseconds(), 3)}`
  ],
  [
    1000 * 60 * 60,
    d => `${leftPad(d.getHours())}:${leftPad(d.getMinutes())}:${leftPad(d.getSeconds())}`
  ],
  [
    1000 * 60 * 60 * 24,
    d => `${leftPad(d.getHours())}:${leftPad(d.getMinutes())}`
  ],
  [
    1000 * 60 * 60 * 24 * 30,
    d => `${d.getFullYear()}-${leftPad(d.getMonth() + 1)}-${leftPad(d.getDate())} ${leftPad(d.getHours())}:${leftPad(d.getMinutes())}`,
  ],
  [
    1000 * 60 * 60 * 24 * 30,
    d => `${d.getFullYear()}-${leftPad(d.getMonth() + 1)}-${leftPad(d.getDate())}`,
  ],
]
